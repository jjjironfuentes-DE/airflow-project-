from datetime import datetime
from zoneinfo import ZoneInfo

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


def choose_load():
    today = datetime.now(ZoneInfo("Europe/Berlin"))

    if today.weekday() == 0:
        print("Monday: selecting full load")
        return "full_load"

    print("Not Monday: selecting incremental load")
    return "incremental_load"


with DAG(
    dag_id="branching_pipeline",
    start_date=pendulum.datetime(
        2025, 1, 1, tz="Europe/Berlin"
    ),
    schedule=None,
    catchup=False,
    tags=["beginner", "branching"],
) as dag:

    branch = BranchPythonOperator(
        task_id="choose_load",
        python_callable=choose_load,
    )

    full_load = BashOperator(
        task_id="full_load",
        bash_command="echo 'Running FULL load'",
    )

    incremental_load = BashOperator(
        task_id="incremental_load",
        bash_command="echo 'Running INCREMENTAL load'",
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    branch >> [full_load, incremental_load]
    [full_load, incremental_load] >> end
