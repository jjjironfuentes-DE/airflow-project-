# Airflow Data Engineering Labs

Practice projects using Apache Airflow, Python, Docker, APIs, CSV, JSON and Pandas.

## Labs

- API Health Monitor: checks website availability and records health results.
- IMDb TV Analysis: retrieves popular TV titles using RapidAPI.
- IMDb Pandas Analysis: transforms IMDb JSON data into a CSV using Pandas.
- Retail Ingestion: validates sales data and creates a cleaned dataset.
- Retail Sales Alerts: generates sales reports and supports email alerts.
- My First DAG: introductory Airflow practice.

## Requirements

- Docker Desktop
- Docker Compose
- Python 3 for the local Pandas script
- Your own RapidAPI key for IMDb
- SMTP credentials if you want to test email notifications

## Setup

1. Clone this repository.
2. Create your local environment file by copying `.env.example` to `.env`.
3. Add your own credentials to `.env`. Never publish this file.
4. Start Airflow with `docker compose up -d`.
5. Open http://localhost:8080.
6. Trigger the lab DAGs from the Airflow interface.

## Stop and restart

Stop the services without deleting their data:

`docker compose stop`

Start them again:

`docker compose start`

## Local Pandas analysis

Create a virtual environment, activate it and install Pandas:

`python3 -m venv .venv`

`source .venv/bin/activate`

`python3 -m pip install pandas`

Run the analysis:

`python analyze_imdb_data.py`

The script reads `data/processed/top_5_tv_shows.json` and writes `data/processed/top_5_tv_shows.csv`.

## Security

- Keep `.env`, API keys, email passwords and database credentials private.
- `.env.example` must contain placeholders only.
- Do not commit local Airflow logs, configuration files or private reports.