# SkyListen roadmap (written Thu 2026-11-12)

This replaces the loose "what next" talk. `PLAN.md` stays as the original design. This file is the order of work from here. Follow it strictly.

## Objective
Give SkyListen a watchlist of brands. It shows volume, sentiment and spikes per brand, and warns early when chatter jumps or turns negative. Everything runs in the cloud, not on a laptop.

Honest limits, to be stated in the README: Bluesky is small and skewed, so many brands get little chatter. Sentiment accuracy will be measured on our own labeled sample and reported as measured. This is a credible demo, not a Brandwatch replacement.

## Rules
1. Work only on the current phase. Nothing from a later phase starts early.
2. A new idea goes into the parking lot at the bottom. It does not go into the current phase.
3. Changing this plan needs Glen's explicit yes, and the change is written here first.
4. A phase is done when its "done when" line is true and everything is committed and pushed.
5. Time box: the whole roadmap is about 25 to 30 working hours (estimate). When Phase 4 is done, stop building and apply for jobs.

## Where we are
Done and pushed, running on the laptop: collector, hourly bronze files, S3 backup of raw files, Airflow DAGs (check and upload; silver, delete cleanup, dbt run, dbt test), silver in Parquet, gold tables in DuckDB, freshness check, 14-day retention, Streamlit dashboard.

Not in the cloud yet: everything except raw backups in S3.

## Research notes (checked 2026-11-12)
- AWS Free plan: $100 credits at sign-up, up to $100 more for five onboarding tasks. The account closes after 6 months or when credits run out, whichever is first. For accounts created after 2025-07-15, eligible EC2 types are t3.micro, t3.small, t4g.micro, t4g.small, c7i-flex.large, m7i-flex.large. Verify in the console launch screen ("Free tier eligible") before choosing. Sources: [AWS Free Tier update](https://aws.amazon.com/blogs/aws/aws-free-tier-update-new-customers-can-get-started-and-explore-aws-with-up-to-200-in-credits), [EC2 free tier usage](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-free-tier-usage.html).
- Prices are from third-party sites and not exact: t4g.micro about $0.008 an hour (about $6 a month), t4g.medium about $0.034 to $0.037 an hour (about $25 to $27 a month). Check the AWS pricing page. Source: [aws-pricing.com t4g.micro](https://aws-pricing.com/t4g.micro.html), [aws-pricing.com t4g.medium](https://aws-pricing.com/t4g.medium.html).
- Public IPv4 address: $0.005 an hour (about $3.65 a month). EBS gp3 disk: $0.08 per GB-month (30 GB is about $2.40). Source: [AWS public IPv4 charge](https://aws.amazon.com/blogs/aws/new-aws-public-ipv4-address-charge-public-ip-insights).
- Airflow's Docker Compose docs ask for at least 4 GB of memory, 8 GB preferred. The quick-start Compose file is not meant for production. Source: [Airflow docs](https://airflow.apache.org/docs/apache-airflow/2.7.2/howto/docker-compose/index.html).
- Sentiment model candidate: `cardiffnlp/twitter-roberta-base-sentiment-latest`, a RoBERTa-base model trained on about 124 million tweets, labels negative, neutral, positive. No official accuracy was found, so we measure it ourselves. Source: [Hugging Face model card](https://huggingface.co/cardiffnlp/twitter-roberta-base-latest-tweet-sentiment).
- Not researched yet: GLiNER, Athena pricing, m7i-flex and c7i-flex prices.

## Phase 1: Collector in the cloud (about 3 hours)
Why first: it is the one part that lost data (about 41 hours).
- Glen: in the AWS console, check the budget alarm, then create one small free-plan-eligible machine (t4g.micro or t3.micro), a key pair, SSH only from his IP.
- On the machine: install Docker, clone the repo, run `collect.py` as a Docker service with `restart: always`.
- Files upload to S3 hourly from the machine (the existing S3 bucket).
Done when: the collector runs 48 hours with no gap in the hourly files, laptop collector switched off.

## Phase 2: Pipeline in the cloud (about 8 hours)
- Check memory first. Airflow needs 4 GB or more. Decide the machine size from the console's eligible list and real prices, and write the decision here before launching.
- Move Airflow, the Spark job, dbt and the dashboard to the cloud machine.
- Silver and gold written to S3. Silver upload task added to the DAG.
- Back up and upload the Nov 5 bronze files (they were never uploaded).
- Run it for at most 2 to 3 weeks, then stop the machine when not needed.
Done when: the hourly DAG runs in the cloud for 48 hours, all green, laptop off, dashboard reachable.

## Phase 3: Tier B, in this order (about 10 hours)
1. Watchlist: brands in a dbt seed file (about 20 brands), mentions and spikes per brand.
2. Sentiment on posts that mention a watchlist brand only, with the open-source model above. Measure accuracy on about 200 posts Glen labels by hand. Measure cost and speed.
3. Spike and negative-trend alert on the dashboard.
4. Data contracts (Pandera) and a data-quality results table with a page on the dashboard.
5. Terraform for S3, IAM and the machine.
6. GitHub Actions: lint, tests, dbt build on a sample.
Done when: every item above works and is committed, and the README reports the measured numbers.

## Phase 4: Ship it (about 3 hours)
- README: diagram, dashboard screenshots, measured numbers, known limits.
- Resume and LinkedIn lines, using only measured numbers.
- Stop the cloud machine if credits are running low.
Done when: README is final and the resume line is written. Then apply for jobs.

## Parking lot (not in the plan, do not start)
- Athena (listed in PLAN.md; DuckDB already works; decide only after Phase 2)
- Star schema (fact and dimension tables)
- Automatic brand discovery with NER (spaCy test found real brands but also politics and junk; GLiNER untested)
- Kafka, Spark Structured Streaming, the agent or MCP server, Snowflake (Tier C)
- Incremental dbt models
- Rename the `silver_hourly` DAG, remove hard-coded paths
- Merge small Parquet files
- Email or chat alerts for the freshness check
