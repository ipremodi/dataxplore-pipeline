from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "dataxplore",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dataxplore_scoring",
    default_args=default_args,
    description="Score and classify ingested posts using Gemini",
    schedule_interval="30 */6 * * *",  # 30 mins after ingestion
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dataxplore", "scoring"],
) as dag:

    score = BashOperator(
        task_id="run_scoring",
        bash_command="cd /opt/airflow && python /opt/dataxplore/scoring/scorer.py",
    )