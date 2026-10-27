import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def calculate_total_sales(ti):
    sale_amounts = [1200, 50, 80, 350, 25]
    total = sum(sale_amounts)

    print(f"Calculated total sales: {total}")

    ti.xcom_push(key="total_sales", value=total)


def read_total_sales(ti):
    total = ti.xcom_pull(
        task_ids="calculate_total_sales",
        key="total_sales",
    )

    if total is None:
        raise ValueError("No total sales value was received from XCom.")

    print(f"Total sales received from XCom: {total}")


with DAG(
    dag_id="xcom_sales_pipeline",
    start_date=pendulum.datetime(
        2025, 1, 1, tz="Europe/Berlin"
    ),
    schedule=None,
    catchup=False,
    tags=["beginner", "xcom"],
) as dag:

    calculate = PythonOperator(
        task_id="calculate_total_sales",
        python_callable=calculate_total_sales,
    )

    read = PythonOperator(
        task_id="read_total_sales",
        python_callable=read_total_sales,
    )

    calculate >> read
