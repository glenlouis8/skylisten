from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

from datetime import datetime

with DAG(
    dag_id="hello",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
):
    BashOperator(
        task_id="say_hello",
        bash_command="echo hello from skylisten",
    )
