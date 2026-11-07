from datetime import timedelta
import gzip
from airflow.sdk import DAG, task, get_current_context
import os
import json
from datetime import datetime, timedelta

with DAG(
    dag_id="bronze_check",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
):

    @task
    def check_file_exists():
        context = get_current_context()

        hour = context["logical_date"] - timedelta(hours=1)
        path = f"/opt/airflow/data/bronze/posts-{hour:%Y-%m-%d-%H}.jsonl.gz"
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        return path

    @task
    def check_file_healthy(path):
        count = 0
        with gzip.open(path, "rt") as f:
            for line in f:
                event = json.loads(line)
                if count == 0:
                    for key in ["did", "time_us", "kind", "cursor"]:
                        if key not in event:
                            raise ValueError(f"missing key {key}")
                count += 1
        if count < 50000:
            raise ValueError(f"only {count} posts in {path}")
        return count

    path = check_file_exists()
    check_file_healthy(path)
