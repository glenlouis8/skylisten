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
Airflow (Docker Compose, hourly)  ->  bronze_check DAG
        |
        v   (planned)
S3 bronze -> PySpark silver -> dbt gold -> Athena -> Streamlit
```

## Status

| Piece | State |
|---|---|
| Collector (websocket, hourly gzip JSONL, cursor resume, auto-reconnect) | working, running continuously |
| Airflow in Docker Compose (LocalExecutor + Postgres) | working |
| `bronze_check` DAG: last hour's file exists, is valid gzip, has the key fields, has enough posts | working |
| S3 bronze, PySpark silver, dbt gold, dashboard | planned |
| Sentiment / topic labels, streaming, agent | planned, after the pipeline works |

## What I measured

- Roughly 117,000 to 192,000 posts per hour, from a handful of hourly files I counted.
- About 40 to 50 MB per hour compressed with gzip, around 1 GB a day for everything.
- The health check caught a real truncated file left by a collector crash.

## Data handling

The feed contains personal data (every post has a user ID). Rules for this project:

- Read only: the code never posts, likes, follows or messages.
- Raw and cleaned posts are never committed (`data/` is in `.gitignore`) and never shared.
- Delete events are kept in bronze and must be honored downstream (planned for silver).
- User IDs get hashed in silver, and gold has none (planned).
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

## Known issues

- Files are named by arrival time, not post time, so a replay after downtime lands in one big file. Silver will sort posts by their own timestamp.
- The collector rewrites its cursor file on every post. Fine for now, wasteful at scale.
