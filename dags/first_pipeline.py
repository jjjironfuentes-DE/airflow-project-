import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="first_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Berlin"),
    schedule="0 8 * * *",
    catchup=False,
    tags=["beginner", "practice"],
) as dag:

    start_pipeline = BashOperator(
        task_id="start_pipeline",
        bash_command="echo 'Data Pipeline Started'",
    )

    wait_10_seconds = BashOperator(
        task_id="wait_10_seconds",
        bash_command="sleep 10",
    )

    end_pipeline = BashOperator(
        task_id="end_pipeline",
        bash_command="echo 'Data Pipeline Completed'",
    )

    start_pipeline >> wait_10_seconds >> end_pipeline
