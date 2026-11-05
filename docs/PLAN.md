# SkyListen: project plan (decided Wed 2026-11-04)

## One line
A data platform that collects live public Bluesky posts, cleans and organizes them, checks their quality, and answers: "What are people saying about a brand or topic right now, and is it turning negative?"

## Real-world use
This is "social listening". Companies pay tools like Brandwatch and Sprout Social to watch what the public says about their brand, catch a complaint wave early, and see which topics are trending. Stakeholder for this project: a brand or comms team.

## Why this project (Glen's goal)
Show ALL of Glen's skills in one project with a real use case:
| Skill | Where it shows |
|---|---|
| Data engineering (new, main goal) | collector, Airflow, S3 bronze/silver/gold, PySpark, dbt, warehouse |
| Streaming (new) | Kafka on the live feed, near-real-time spike alerts |
| Data quality (his Wipro background) | deletes, duplicates, spam/bots, language, timestamps, data contracts |
| ML / LLM (his strength) | sentiment and topic labels on posts |
| Agents / MCP (his edge) | an agent that answers "what are people saying about Nvidia today?" over the clean tables |
| MLOps | serving the labeler, cost per day, monitoring |

ORDER IS STRICT: the data engineering pipeline first. The AI layer goes on top only after the pipeline works.

## Data source (tested live 11/4)
- Bluesky Jetstream, free, no API key, websocket:
  `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post`
- Test 11/4: 503 new posts in 10 seconds (about 4 million a day). JSON events. Kinds include new posts (commit create) and deletions (commit delete).
- Docs: github.com/bluesky-social/jetstream
- No ready-made history: history starts the day the collector starts. So the collector runs EARLY (Day 2) and keeps running.
- Keep storage small: English posts only, only needed fields, compressed files. Raw (bronze) kept a limited number of days.

## Architecture
Collector (Python, websocket, saves one compressed JSONL file per hour, remembers where it stopped with a cursor)
-> S3 bronze (raw files, untouched)
-> Airflow DAG, hourly: check the new file, run PySpark
-> PySpark bronze to silver (parse JSON, dedupe, apply deletes, language filter, clean timestamps, Parquet partitioned by date/hour)
-> dbt silver to gold (posts per topic per hour, brand mentions per day, top terms, star schema: fact_posts, dim_author, dim_date, dim_topic)
-> Athena (and later Snowflake) to query
-> Streamlit dashboard
Quality gates: data contract check before silver, dbt tests after each layer, freshness check (alert if no new data in 2 hours), volume check (alert if posts per hour drop sharply).

## Tiers and dates (honest estimates, about 3 focused hours a day)
### Tier A: resume-ready DE pipeline (about 2 weeks, target Wed 11/18)
- Day 1: concepts, repo, README, Docker check, look at 5 live posts.
- Day 2: collector script, hourly files on local disk. Leave it running.
- Days 3-4: Docker Compose with Airflow. First DAG. Learn DAG, task, operator, schedule, retries.
- Days 5-6: S3 bronze (AWS account, $5 budget alarm FIRST), DAG uploads hourly files.
- Days 7-8: PySpark bronze to silver.
- Days 9-10: dbt gold models + tests.
- Days 11-12: Athena + Streamlit dashboard, README with diagram, first real numbers.
Resume line allowed only after Tier A runs end to end. Numbers only from real runs.

### Tier B: quality + AI layer (about 1 week)
- Data contracts (Pandera), DQ results table, DQ page on dashboard, freshness and volume alerts.
- Sentiment and topic labels on a sample of posts (LLM or small model), stored as a gold table. Measure cost per day.
- Terraform for S3/IAM/Athena. GitHub Actions: lint, tests, dbt build on a sample.

### Tier C: streaming + agent (stretch)
- Kafka (or Redpanda) between collector and processing, Spark Structured Streaming, near-real-time "spike" alerts.
- Agent / MCP server that answers questions over the gold tables.
- Snowflake trial: load gold, run dbt-snowflake, compare with Athena.

## Data quality and privacy rules (also interview stories)
The data DOES contain personal data (PII): every post has a user ID (DID) linked to a real account, and post text can contain names, emails, locations. Public posts still count as personal data under laws like GDPR. Bluesky Developer Guidelines (read 11/4, bsky.network/docs/developer-guidelines) require: delete content when the user deletes it, reasonable security for user data, no spam or automated interactions.
Project rules:
- Honor delete events: if a user deletes a post, remove it from bronze, silver and gold.
- Never commit raw or cleaned posts to GitHub (`data/` in .gitignore). Never publish or share the dataset.
- Hash user IDs (DIDs) in silver; gold has no user IDs at all.
- Dashboard shows aggregates (counts, trends, sentiment) only, never individual posts, handles or users.
- Retention: delete raw bronze files after 14 days.
- Read only: never post, like, follow or message from code. Never track or profile specific people.
- Dedupe on reconnect using the cursor and post IDs.
- Glen handled PII at Wipro; this is a good interview story.

## Cost
AWS budget alarm at $5 before creating anything. Develop locally first. LLM labeling on a sample only, with a daily cost cap. Expected spend under $10 to $15 total.

## Honest limits
- Many DE job descriptions ask for 3+ years. This project improves odds, it does not remove that gate.
- "Show all skills" is how projects never finish. Tier A first, no exceptions.
- The data source is free and run by Bluesky. If it goes down, the collector's saved files still let the pipeline run.
