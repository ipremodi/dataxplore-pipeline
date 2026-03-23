from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "dataxplore",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dataxplore_ingestion",
    default_args=default_args,
    description="Ingest posts from source Telegram channels",
    schedule_interval="0 */6 * * *",  # every 6 hours
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dataxplore", "ingestion"],
) as dag:

    ingest = BashOperator(
        task_id="run_ingestion",
        bash_command="cd /opt/airflow && python /opt/dataxplore/ingestion/ingest.py",
    )