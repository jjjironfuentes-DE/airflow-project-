import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

FILE_PATH = "/opt/airflow/data/incoming_data/customer_data.csv"

with DAG(
    dag_id="file_sensor_pipeline",
    start_date=pendulum.datetime(
        2025, 1, 1, tz="Europe/Berlin"
    ),
    schedule=None,
    catchup=False,
    tags=["beginner", "file-sensor"],
) as dag:

    wait_for_customer_file = FileSensor(
        task_id="wait_for_customer_file",
        filepath="incoming_data/customer_data.csv",
        fs_conn_id="fs_sales_lab",
        poke_interval=5,
        timeout=300,
    )

    process_customer_file = BashOperator(
        task_id="process_customer_file",
        bash_command=(
            "set -e; "
            f"test -f {FILE_PATH}; "
            "echo 'Customer file detected. Contents:'; "
            f"cat {FILE_PATH}; "
            "echo 'File processed successfully.'"
        ),
    )

    wait_for_customer_file >> process_customer_file
