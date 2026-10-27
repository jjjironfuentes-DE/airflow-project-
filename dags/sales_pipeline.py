from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_source_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_source_project/profiles"
LOAD_SCRIPT = "/opt/airflow/dbt_source_project/scripts/load_sales_raw.py"

with DAG(
    dag_id="sales_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "duckdb", "sales"],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    load_csv = BashOperator(
        task_id="load_csv",
        bash_command=f"python {LOAD_SCRIPT}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> load_csv >> dbt_run >> dbt_test >> end
