import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="etl_workflow",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Berlin"),
    schedule=None,
    catchup=False,
    tags=["beginner", "etl"],
) as dag:

    extract_data = BashOperator(
        task_id="extract_data",
        bash_command="echo 'Extracting data from the source'",
    )

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command="echo 'Transforming the extracted data'",
    )

    load_data = BashOperator(
        task_id="load_data",
        bash_command="echo 'Loading transformed data into the destination'",
    )

    extract_data >> transform_data >> load_data
