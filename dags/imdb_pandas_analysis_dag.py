
import json
import logging
from pathlib import Path

import pandas as pd
import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator


INPUT_FILE = Path("/opt/airflow/data/processed/top_5_tv_shows.json")
OUTPUT_FILE = Path("/opt/airflow/data/processed/top_5_tv_shows.csv")


def analyze_imdb_data():
    if not INPUT_FILE.exists():
        raise AirflowException(
            f"Input file not found: {INPUT_FILE}"
        )

    with open(INPUT_FILE, "r", encoding="utf-8") as file:
        data = json.load(file)

    if not isinstance(data, list) or not data:
        raise AirflowException("IMDb JSON is empty or invalid.")

    df = pd.DataFrame(data)

    required_columns = ["rank", "title", "year", "imdb_id", "url"]
    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise AirflowException(
            f"Missing columns: {missing_columns}"
        )

    df = df[required_columns]
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    logging.info("IMDb analysis completed.")
    logging.info("Number of shows: %s", len(df))
    logging.info("CSV saved to: %s", OUTPUT_FILE)
    logging.info("\n%s", df.to_string(index=False))


with DAG(
    dag_id="imdb_pandas_analysis",
    description="Analyze IMDb TV data using Pandas and export CSV",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["imdb", "pandas", "data-analysis"],
) as dag:

    analyze_task = PythonOperator(
        task_id="analyze_imdb_data",
        python_callable=analyze_imdb_data,
    )
