import csv
from decimal import Decimal
from pathlib import Path

import duckdb

CSV_PATH = Path("/opt/airflow/data/airflow_dbt_source_lab/sales.csv")
DB_PATH = Path("/opt/airflow/data/airflow_dbt_source_lab/warehouse.duckdb")


def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as file:
        rows = list(csv.DictReader(file))

    if not rows:
        raise ValueError("The sales CSV contains no data rows.")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(DB_PATH))

    try:
        connection.execute("CREATE SCHEMA IF NOT EXISTS public")
        connection.execute("DROP TABLE IF EXISTS public.sales_raw")
        connection.execute("""
            CREATE TABLE public.sales_raw (
                order_id INTEGER,
                customer VARCHAR,
                city VARCHAR,
                product VARCHAR,
                quantity INTEGER,
                price DECIMAL(10, 2)
            )
        """)

        records = [
            (
                int(row["order_id"]),
                row["customer"],
                row["city"],
                row["product"],
                int(row["quantity"]),
                Decimal(row["price"]),
            )
            for row in rows
        ]

        connection.executemany(
            "INSERT INTO public.sales_raw VALUES (?, ?, ?, ?, ?, ?)",
            records,
        )

        count = connection.execute(
            "SELECT COUNT(*) FROM public.sales_raw"
        ).fetchone()[0]

        print(f"Loaded {count} rows into public.sales_raw")
        print(connection.execute(
            "SELECT * FROM public.sales_raw ORDER BY order_id"
        ).fetchdf().to_string(index=False))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
