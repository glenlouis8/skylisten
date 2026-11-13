# SkyListen

SkyListen is a data pipeline that collects live public Bluesky posts, cleans and organizes them, and checks their quality.

The question it answers: what are people saying about a brand or topic right now, and is it turning negative?

I first planned to use GitHub event data, but recent GitHub Archive data dropped to about 3,000 events an hour and the crawler is no longer maintained, so I switched to Bluesky's live feed.

## Architecture

```
Bluesky Jetstream (websocket)
        |
        v
collect.py  ->  data/bronze/posts-YYYY-MM-DD-HH.jsonl.gz   (raw, one file per UTC hour)
        |
        v
Airflow (Docker Compose, hourly)  ->  bronze_check DAG  ->  upload to S3 bronze
        |
        v
Airflow silver_hourly DAG, one task per step, each in its own Docker container:
    build_silver (PySpark, Parquet by date/hour)
      -> apply_deletes (cleanup)
      -> dbt_run (gold tables in DuckDB)
      -> dbt_test (quality test)
        |
        v
Streamlit dashboard (reads gold)
```

## Status

| Piece | State |
|---|---|
| Collector (websocket, hourly gzip JSONL, cursor resume, auto-reconnect) | working, running continuously |
| Airflow in Docker Compose (LocalExecutor + Postgres) | working |
| `bronze_check` DAG: last hour's file exists, is valid gzip, has the key fields, has enough posts | working |
| Hourly upload of checked files to S3 bronze | working |
| Freshness check: task fails if no bronze file was touched in 2 hours | working (passing side tested) |
| 14-day retention: deletes local bronze older than 14 days only if S3 has the same file at the same size | working, delete logic tested on scratch copies |
| PySpark silver (`spark/silver.py`): fixed schema, English + non-empty text, hashed author id, in-file deletes, Parquet by date/hour | working |
| `spark/apply_deletes.py`: removes posts deleted hours later from old silver folders | working |
| dbt + DuckDB gold: `posts_per_hour`, `brand_mentions_per_day` (nvidia, apple, tesla), one volume test | working |
| `silver_hourly` DAG: build_silver, apply_deletes, dbt_run, dbt_test, chained, each a DockerOperator | working, runs hourly |
| Streamlit dashboard (`dashboard/app.py`): brand mentions per day, complete days only | working, started by hand |
| Spike detection, sentiment, Athena | planned |
| Sentiment / topic labels, streaming, agent | planned, after the pipeline works |

## What I measured

- Roughly 117,000 to 192,000 posts per hour, from a handful of hourly files I counted.
- About 40 to 50 MB per hour compressed with gzip, around 1 GB a day for everything.
- The health check caught a real truncated file left by a collector crash.
- One hourly file (about 54 MB gzipped) becomes about 19 MB of silver Parquet.
- About 64% of posts in the hours I tested were tagged English (124,983 of 193,870 in one hour). Posts with no language tag are dropped.
- About 1 in 64 posts is deleted within the same hour it was written (1,878 of 120,225 in one hour).
- With silver built for 7 hours, 1,031 of 776,186 posts (0.13%) had been deleted after their hour was already processed. `apply_deletes.py` removed them. Average age at delete in that sample was 2.9 hours; the sample is too short to know the longest.

- Gold test caught a wrong threshold: copying the bronze limit (50,000 posts/hour, all languages) failed on 8 quiet night hours. English-only gold is about 60% of that, so the limit is 30,000 (quietest real hour: 43,087).
- Brand mention counts so far: 5 days of data, 82 hours of silver. Counts use whole-word, case-insensitive matches, so "apple" also counts the fruit.

## Data handling

The feed contains personal data (every post has a user ID). Rules for this project:

- Read only: the code never posts, likes, follows or messages.
- Raw and cleaned posts are never committed (`data/` is in `.gitignore`) and never shared.
- Delete events are kept in bronze. Silver applies them in two places: inside each job, and later by `apply_deletes.py`, which checks a saved list of removed posts against all of silver.
- User IDs are hashed in silver with a secret salt (in `spark/.env`, never committed). Gold will have none (planned).
- The dashboard will show aggregates only, never individual posts or users (planned).
- Local raw files are deleted after 14 days, but only when a same-size copy is in S3. S3 copies and silver/gold are not expired yet, so a deleted post can live on in those until that is built.

## Run it

Collector, from the project folder:

```bash
caffeinate -i uv run collect.py
```

Airflow, from the `airflow` folder (needs an `.env` with `AIRFLOW_UID` and `FERNET_KEY`):

```bash
docker compose up airflow-init
docker compose up -d
```

Web UI at http://localhost:8080.

Dashboard (stop it before running dbt by hand, since it can lock the gold file):

```bash
uv run streamlit run dashboard/app.py
```

dbt image, built once, used by Airflow:

```bash
docker build -t skylisten-dbt dbt
```

Silver, for one UTC hour (needs `spark/.env` with `HASH_SALT`, and `data/silver` to exist):

```bash
docker run --rm --env-file spark/.env -v "$PWD/data:/data:ro" -v "$PWD/data/silver:/silver" -v "$PWD/spark:/app" apache/spark:4.3.0-python3 /opt/spark/bin/spark-submit /app/silver.py 2026-11-07-14
```

Delete cleanup:

```bash
docker run --rm -v "$PWD/data/silver:/silver" -v "$PWD/spark:/app" apache/spark:4.3.0-python3 /opt/spark/bin/spark-submit /app/apply_deletes.py
```

## Known issues

- Files before Nov 6 were never uploaded to S3, so retention will refuse to delete them (the task turns red) until they are backfilled.
- Airflow has write access to `data/` so it can delete old raw files.
- Freshness only raises a failed task. There is no email or chat alert yet.

- Files are named by arrival time, not post time, so a replay after downtime lands in one big file. Silver will sort posts by their own timestamp.
- The collector rewrites its cursor file on every post. Fine for now, wasteful at scale.
- Silver for hour H reads files H and H+1, so it must run after hour H+1 has finished. The DAG builds the hour two hours back for that reason.
- Airflow starts Spark through the Docker socket (`/var/run/docker.sock` mounted into Airflow). That gives Airflow control of the host's Docker. Fine on a laptop, not something to copy to production.
- The `silver_hourly` DAG has `/Users/glenlouis/Coding/skylisten` hard-coded for the volume mounts.
- Silver writes about 10 small Parquet files per hour. Fewer, larger files would query faster.
- Silver reads the local bronze copy, not S3 yet.
- Posts with no language tag are dropped, which loses some real English posts.
- Image-only posts are dropped. Image content is not analyzed.
- `apply_deletes.py` swaps folders in two steps, so a crash in the middle could leave a folder missing. Bronze can rebuild it.
- About 41 hours of collection were lost (Nov 11 05 UTC to Nov 12 21 UTC) because the collector was stopped. Jetstream replayed about one hour on restart.
- `apply_deletes.py` failed on every scheduled run from Nov 9 to Nov 12 (62 failed runs) because Spark read the folder name `hour=09` as the number 9. Fixed by turning off partition type inference. `build_silver` was fine the whole time.
- `brand_mentions_per_day` rescans all of silver each run (about 20 to 30 seconds for 7 million posts). Fine now, would need incremental models at larger sizes.
- Streamlit is started by hand and closes the gold database after each read, so a refresh during the dbt build can show an error.
- Silver has holes if Airflow or Docker was off, because `catchup=False`. The dashboard only draws days with all 24 hours, which exposed two missing hours that were then backfilled by hand.
- The DAG is still named `silver_hourly` although it now also runs dbt. Its volume paths are hard-coded to this laptop.
