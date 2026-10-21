
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="my_first_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def my_first_dag():

    @task
    def say_hello():
        print("Hello! My first Airflow pipeline is working!")

    say_hello()


my_first_dag()

