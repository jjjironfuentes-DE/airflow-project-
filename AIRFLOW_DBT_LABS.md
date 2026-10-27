# Airflow and dbt Hands-on Labs

This document describes the Airflow and dbt exercises completed in this project.

## 1. Apache Airflow

The project uses Apache Airflow with Docker Compose and a custom Docker image.

### DAG exercises

- `first_pipeline.py`: basic scheduled pipeline.
- `etl_workflow.py`: extract, transform, and load workflow.
- `xcom_sales_pipeline.py`: task communication using XCom.
- `dynamic_country_pipeline.py`: dynamically mapped tasks for multiple countries.
- `file_sensor_pipeline.py`: waits for an incoming CSV file.
- `branching_pipeline.py`: selects a full or incremental load based on the weekday.
- `dbt_pipeline.py`: runs dbt models and tests.
- `sales_pipeline.py`: loads raw sales data and executes dbt transformations.

### Run the services

From the project root, run:

    docker compose up -d --build
    docker compose ps

## 2. dbt with DuckDB

The project includes two dbt exercises.

### Project A: `dbt_project/`

This project reads sales data from `data/airflow_dbt_lab/sales.csv`.

The model calculates estimated tax and the total price including tax. A SQL test checks that totals are positive.

### Project B: `dbt_source_project/`

This project demonstrates a source-to-mart workflow:

1. Load raw sales data into DuckDB.
2. Define the raw table as a dbt source.
3. Build the `stg_sales` staging model.
4. Build the `customer_sales` mart using `ref()`.
5. Run the configured data quality tests.

Input data: `data/airflow_dbt_source_lab/sales.csv`.

## 3. Configuration and dependencies

- `Dockerfile`: builds a custom Airflow image.
- `requirements.txt`: defines the additional Python packages.
- `docker-compose.yaml`: configures Airflow services and project mounts.
- `profiles.example.yml`: example dbt profile configuration.

Create a local `profiles.yml` from the corresponding example when configuring dbt. Adjust paths to match your environment.

## 4. Local files and secrets

Do not commit `.env`, real dbt `profiles.yml` files, local DuckDB databases, generated dbt artifacts, or runtime-only input files.

These files should remain excluded through `.gitignore`. Keep credentials and passwords out of version control.
