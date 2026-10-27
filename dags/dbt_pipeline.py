from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project/profiles"

with DAG(
    dag_id="sales_dbt_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "duckdb"],
) as dag:

    check_dbt = BashOperator(
        task_id="check_dbt",
        bash_command=(
            f"dbt debug "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    build_models_and_test = BashOperator(
        task_id="build_models_and_test",
        bash_command=(
            f"dbt build "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    check_dbt >> build_models_and_test
