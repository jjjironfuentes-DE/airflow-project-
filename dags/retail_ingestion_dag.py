from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

RAW_FILE = "/opt/airflow/data/raw/daily_sales.csv"
PROCESSED_FILE = "/opt/airflow/data/processed/cleaned_sales.csv"

default_args = {
    "owner": "retail_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def validate_sales_data():
    df = pd.read_csv(RAW_FILE)

    required_columns = [
        "Order_ID", "Product", "Quantity", "Price", "City"
    ]

    if not all(column in df.columns for column in required_columns):
        raise ValueError("Required columns are missing!")

    if df.isnull().values.any():
        raise ValueError("Null values detected!")

    if (df["Price"] < 0).any():
        raise ValueError("Negative prices found!")

    if (df["Quantity"] < 0).any():
        raise ValueError("Negative quantities found!")

    print("Validation successful!")


def transform_sales_data():
    df = pd.read_csv(RAW_FILE)
    df["Total_Sales"] = df["Quantity"] * df["Price"]

    Path(PROCESSED_FILE).parent.mkdir(
        parents=True, exist_ok=True
    )
    df.to_csv(PROCESSED_FILE, index=False)

    print("Transformation completed!")


with DAG(
    dag_id="retail_ingestion_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["retail", "sales"],
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_sales_file",
        filepath=RAW_FILE,
        poke_interval=30,
        timeout=3600,
        mode="reschedule",
    )
    
    validate = PythonOperator(
        task_id="validate_sales_data",
        python_callable=validate_sales_data,
    )

    transform = PythonOperator(
        task_id="transform_sales_data",
        python_callable=transform_sales_data,
    )

    wait_for_file >> validate >> transform
