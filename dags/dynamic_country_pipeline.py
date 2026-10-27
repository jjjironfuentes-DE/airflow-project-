import time
import pendulum

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="dynamic_country_pipeline",
    start_date=pendulum.datetime(
        2025, 1, 1, tz="Europe/Berlin"
    ),
    schedule=None,
    catchup=False,
    tags=["beginner", "dynamic-mapping"],
) as dag:

    @task
    def get_countries():
        return ["Germany", "France", "Spain", "Nicaragua"]

    @task
    def process_country(country: str):
        print(f"Starting processing for {country}")
        time.sleep(3)
        print(f"Finished processing for {country}")

    countries = get_countries()

    process_country.expand(country=countries)
