from datetime import timedelta
import gzip
from airflow.sdk import DAG, task, get_current_context
import os
import json
from datetime import datetime, timedelta
import time
import boto3
from botocore.exceptions import ClientError

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

    @task
    def upload_to_s3(path):
        name = os.path.basename(path)
        year, month, day = name.split("-")[1:4]
        key = f"bronze/{year}/{month}/{day}/{name}"
        boto3.client("s3").upload_file(path, os.environ["S3_BUCKET"], key)
        return key

    @task
    def check_collector_fresh():
        folder = "/opt/airflow/data/bronze"
        newest = max(
            os.path.getmtime(os.path.join(folder, f)) for f in os.listdir(folder)
        )
        age_hours = (time.time() - newest) / 3600
        if age_hours > 2:
            raise ValueError(
                f"no new data for {age_hours:.1f} hours, collector may be down"
            )
        return round(age_hours, 2)

    @task
    def delete_old_bronze():
        folder = "/opt/airflow/data/bronze"
        bucket = os.environ["S3_BUCKET"]
        s3 = boto3.client("s3")
        cutoff = time.time() - 14 * 86400
        deleted, kept = 0, []
        for name in os.listdir(folder):
            path = os.path.join(folder, name)
            if not name.startswith("posts-") or os.path.getmtime(path) > cutoff:
                continue
            year, month, day = name.split("-")[1:4]
            key = f"bronze/{year}/{month}/{day}/{name}"
            try:
                size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
            except ClientError:
                kept.append(name)
                continue
            if size != os.path.getsize(path):
                kept.append(name)
                continue
            os.remove(path)
            deleted += 1
        if kept:
            raise ValueError(
                f"{len(kept)} old files not safely in S3, kept: {kept[:3]}"
            )
        return deleted

    path = check_file_exists()
    healthy = check_file_healthy(path)
    uploaded = upload_to_s3(path)
    healthy >> uploaded
    check_collector_fresh()
    delete_old_bronze()
