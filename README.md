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
PySpark silver (Parquet, by date/hour)  ->  apply_deletes cleanup
        |
        v   (planned)
dbt gold -> Athena -> Streamlit
```

## Status

| Piece | State |
|---|---|
| Collector (websocket, hourly gzip JSONL, cursor resume, auto-reconnect) | working, running continuously |
| Airflow in Docker Compose (LocalExecutor + Postgres) | working |
| `bronze_check` DAG: last hour's file exists, is valid gzip, has the key fields, has enough posts | working |
| Hourly upload of checked files to S3 bronze | working |
| PySpark silver (`spark/silver.py`): fixed schema, English + non-empty text, hashed author id, in-file deletes, Parquet by date/hour | working, run by hand for now |
| `spark/apply_deletes.py`: removes posts deleted hours later from old silver folders | working, run by hand for now |
| Airflow triggering Spark, dbt gold, dashboard | planned |
| Sentiment / topic labels, streaming, agent | planned, after the pipeline works |

## What I measured

- Roughly 117,000 to 192,000 posts per hour, from a handful of hourly files I counted.
- About 40 to 50 MB per hour compressed with gzip, around 1 GB a day for everything.
- The health check caught a real truncated file left by a collector crash.
- One hourly file (about 54 MB gzipped) becomes about 19 MB of silver Parquet.
- About 64% of posts in the hours I tested were tagged English (124,983 of 193,870 in one hour). Posts with no language tag are dropped.
- About 1 in 64 posts is deleted within the same hour it was written (1,878 of 120,225 in one hour).
- With silver built for 7 hours, 1,031 of 776,186 posts (0.13%) had been deleted after their hour was already processed. `apply_deletes.py` removed them. Average age at delete in that sample was 2.9 hours; the sample is too short to know the longest.

## Data handling

The feed contains personal data (every post has a user ID). Rules for this project:

- Read only: the code never posts, likes, follows or messages.
- Raw and cleaned posts are never committed (`data/` is in `.gitignore`) and never shared.
- Delete events are kept in bronze. Silver applies them in two places: inside each job, and later by `apply_deletes.py`, which checks a saved list of removed posts against all of silver.
- User IDs are hashed in silver with a secret salt (in `spark/.env`, never committed). Gold will have none (planned).
- The dashboard will show aggregates only, never individual posts or users (planned).
- Raw files are deleted after 14 days (planned).

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

Silver, for one UTC hour (needs `spark/.env` with `HASH_SALT`, and `data/silver` to exist):

```bash
docker run --rm --env-file spark/.env -v "$PWD/data:/data:ro" -v "$PWD/data/silver:/silver" -v "$PWD/spark:/app" apache/spark:4.3.0-python3 /opt/spark/bin/spark-submit /app/silver.py 2026-11-07-14
```

Delete cleanup:

```bash
docker run --rm -v "$PWD/data/silver:/silver" -v "$PWD/spark:/app" apache/spark:4.3.0-python3 /opt/spark/bin/spark-submit /app/apply_deletes.py
```

## Known issues

- Files are named by arrival time, not post time, so a replay after downtime lands in one big file. Silver will sort posts by their own timestamp.
- The collector rewrites its cursor file on every post. Fine for now, wasteful at scale.
- Silver for hour H reads files H and H+1, so it must run after hour H+1 has finished.
- Silver writes about 10 small Parquet files per hour. Fewer, larger files would query faster.
- Silver reads the local bronze copy, not S3 yet.
- Posts with no language tag are dropped, which loses some real English posts.
- Image-only posts are dropped. Image content is not analyzed.
- `apply_deletes.py` swaps folders in two steps, so a crash in the middle could leave a folder missing. Bronze can rebuild it.
