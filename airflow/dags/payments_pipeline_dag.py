from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


def task_failure_alert(context):
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    execution_date = context.get("execution_date")

    print("ALERT: Task Failed!")
    print(f"DAG: {dag_id}")
    print(f"Task: {task_id}")
    print(f"Execution Date: {execution_date}")
    print("Simulated email/slack alert sent to data team.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="payments_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["etl", "payments", "monitoring"],
    description="End-to-end Payments Analytics Pipeline with Airflow monitoring",
) as dag:

    run_pipeline_task = BashOperator(
        task_id="run_pipeline",
        bash_command="cd ~/payments-analytics-platform && python pipelines/run_pipeline.py",
    )

    data_quality_task = BashOperator(
        task_id="run_data_quality_checks",
        bash_command="cd ~/payments-analytics-platform && python pipelines/data_quality_checks.py",
    )

    run_pipeline_task >> data_quality_task
