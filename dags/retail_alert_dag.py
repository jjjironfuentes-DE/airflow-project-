
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

PROCESSED_FILE = "/opt/airflow/data/processed/cleaned_sales.csv"
REPORT_FILE = "/opt/airflow/data/reports/sales_report.txt"
ALERT_FILE = "/opt/airflow/data/reports/sales_alert.txt"

SALES_THRESHOLD = 10000


def check_sales_and_create_report():
    df = pd.read_csv(PROCESSED_FILE)

    total_sales = df["Total_Sales"].sum()
    low_sales = df[df["Total_Sales"] < SALES_THRESHOLD]

    report = [
        "RETAIL SALES MONITORING REPORT",
        f"Report date: {datetime.now():%Y-%m-%d %H:%M:%S}",
        f"Orders processed: {len(df)}",
        f"Total sales: {total_sales:.2f}",
        f"Sales threshold per order: {SALES_THRESHOLD:.2f}",
        f"Low-sales orders: {len(low_sales)}",
        "",
        "ORDERS BELOW THRESHOLD:",
    ]

    if low_sales.empty:
        report.append("No low-sales orders found.")
    else:
        for _, order in low_sales.iterrows():
            report.append(
                f"Order {order['Order_ID']} - "
                f"{order['Product']} - "
                f"Sales: {order['Total_Sales']:.2f}"
            )

    Path(REPORT_FILE).parent.mkdir(parents=True, exist_ok=True)
    Path(REPORT_FILE).write_text(
        "\n".join(report) + "\n",
        encoding="utf-8",
    )

    # Create a separate alert when sales are below the threshold.
    if not low_sales.empty:
        alert = [
            "ALERT: LOW SALES DETECTED",
            f"Report date: {datetime.now():%Y-%m-%d %H:%M:%S}",
            f"Orders below {SALES_THRESHOLD:.2f}: {len(low_sales)}",
            "",
        ]

        for _, order in low_sales.iterrows():
            alert.append(
                f"Order {order['Order_ID']} - "
                f"{order['Product']} - "
                f"Sales: {order['Total_Sales']:.2f}"
            )

        Path(ALERT_FILE).write_text(
            "\n".join(alert) + "\n",
            encoding="utf-8",
        )

        send_email(
            to="jjjironfuentes@gmail.com",
            subject=f"ALERTA: ventas bajas detectadas ({len(low_sales)} pedidos)",
            html_content=(
                "<h2>Alerta de ventas bajas</h2>"
                f"<p>Se detectaron {len(low_sales)} pedidos "
                f"con ventas inferiores a {SALES_THRESHOLD:.2f}.</p>"
                f"<pre>{chr(10).join(alert)}</pre>"
            ),
        )
        print("\n".join(alert))
    else:
        # Remove an old alert if there are no low-sales orders.
        Path(ALERT_FILE).unlink(missing_ok=True)
        print("No low-sales alert: all orders meet the threshold.")

    print("\n".join(report))


with DAG(
    dag_id="retail_sales_alert_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retail", "sales", "monitoring"],
) as dag:

    check_sales = PythonOperator(
        task_id="check_sales_and_create_report",
        python_callable=check_sales_and_create_report,
    )

