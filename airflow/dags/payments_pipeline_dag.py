from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="payments_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["etl", "payments"],
) as dag:

    run_pipeline_task = BashOperator(
        task_id="run_pipeline",
        bash_command="cd ~/payments-analytics-platform/pipelines && python run_pipeline.py",
    )

    data_quality_task = BashOperator(
        task_id="run_data_quality_checks",
        bash_command="cd ~/payments-analytics-platform/pipelines && python data_quality_checks.py",
    )

    run_pipeline_task >> data_quality_task
