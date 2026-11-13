from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT = "/Users/glenlouis/Coding/skylisten"

mounts = [
    Mount(source=f"{PROJECT}/data", target="/data", type="bind", read_only=True),
    Mount(source=f"{PROJECT}/data/silver", target="/silver", type="bind"),
    Mount(source=f"{PROJECT}/spark", target="/app", type="bind"),
]

dbt_mounts = [
    Mount(source=f"{PROJECT}/data", target="/data", type="bind"),
    Mount(source=f"{PROJECT}/dbt", target="/dbt", type="bind"),
]

with DAG(
    dag_id="silver_hourly",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
):
    build_silver = DockerOperator(
        task_id="build_silver",
        image="apache/spark:4.3.0-python3",
        mounts=mounts,
        mount_tmp_dir=False,
        auto_remove="success",
        command=[
            "bash",
            "-c",
            "set -a && . /app/.env && set +a && "
            "/opt/spark/bin/spark-submit /app/silver.py "
            "{{ (logical_date - macros.timedelta(hours=2)).strftime('%Y-%m-%d-%H') }}",
        ],
    )

    apply_deletes = DockerOperator(
        task_id="apply_deletes",
        image="apache/spark:4.3.0-python3",
        mounts=mounts,
        mount_tmp_dir=False,
        auto_remove="success",
        command=["/opt/spark/bin/spark-submit", "/app/apply_deletes.py"],
    )

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="skylisten-dbt",
        mounts=dbt_mounts,
        mount_tmp_dir=False,
        auto_remove="success",
        command="dbt run --profiles-dir /dbt --project-dir /dbt",
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image="skylisten-dbt",
        mounts=dbt_mounts,
        mount_tmp_dir=False,
        auto_remove="success",
        command="dbt test --profiles-dir /dbt --project-dir /dbt",
    )

    build_silver >> apply_deletes >> dbt_run >> dbt_test
