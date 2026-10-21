from datetime import timedelta
import json
import logging
import os
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


from airflow import DAG # type: ignore
from airflow.exceptions import AirflowException # type: ignore
from airflow.providers.standard.operators.python import PythonOperator # type: ignore
from pendulum import datetime # type: ignore




# Read the API key from Docker's environment
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY")


API_URL = (
    "https://imdb8.p.rapidapi.com/"
    "title/get-most-popular-tv-shows"
    "?homeCountry=US&purchaseCountry=US&currentCountry=US"
)

META_URL = "https://imdb8.p.rapidapi.com/title/get-meta-data"


# Keep files in /tmp to match your original DAG and current setup
RAW_FILE = Path("/opt/airflow/data/raw/most_popular_tv.json")
TOP5_FILE = Path("/opt/airflow/data/processed/top_5_tv_shows.json")




# TASK 1: Fetch IMDb TV data
def fetch_imdb_data():
    if not RAPIDAPI_KEY:
        raise AirflowException(
            "RAPIDAPI_KEY is missing from the Airflow environment."
        )


    request = Request(
        API_URL,
        headers={
            "x-rapidapi-host": "imdb8.p.rapidapi.com",
            "x-rapidapi-key": RAPIDAPI_KEY,
        },
        method="GET",
    )


    try:
        with urlopen(request, timeout=30) as response:
            status_code = response.status
            response_text = response.read().decode("utf-8")


    except HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise AirflowException(
            f"IMDb API returned HTTP {error.code}: {error_body[:500]}"
        ) from error


    except (URLError, TimeoutError) as error:
        raise AirflowException(
            f"Could not connect to IMDb API: {error}"
        ) from error


    logging.info("IMDb API returned HTTP %s", status_code)


    try:
        data = json.loads(response_text)
    except json.JSONDecodeError as error:
        raise AirflowException(
            "IMDb API did not return valid JSON."
        ) from error


    with open(RAW_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


    logging.info("Raw response saved to %s", RAW_FILE)
    logging.info("Raw response type: %s", type(data).__name__)


    return "IMDb data fetched successfully"




# TASK 2: Extract and rank the top five TV shows


def extract_top_5():
    if not RAW_FILE.exists():
        raise AirflowException(f"Raw data file not found: {RAW_FILE}")

    with open(RAW_FILE, "r", encoding="utf-8") as file:
        data = json.load(file)

    if not isinstance(data, list) or not data:
        raise AirflowException("IMDb did not return a valid list.")

    headers = {
        "x-rapidapi-host": "imdb8.p.rapidapi.com",
        "x-rapidapi-key": RAPIDAPI_KEY,
    }

    top_5 = []

    for rank, title_url in enumerate(data[:5], start=1):
        # Extract the IMDb ID from a path like /title/tt1234567/
        parts = title_url.strip("/").split("/")
        title_id = parts[-1]

        url = f"{META_URL}?ids={title_id}"
        request = Request(url, headers=headers, method="GET")

        try:
            with urlopen(request, timeout=30) as response:
                metadata = json.loads(response.read().decode("utf-8"))
        except Exception as error:
            raise AirflowException(
                f"Could not retrieve metadata for {title_id}: {error}"
            ) from error

        item = metadata.get(title_id, {})
        title_info = item.get("title", {})

        top_5.append({
            "rank": rank,
            "imdb_id": title_id,
            "title": title_info.get("title", "Title unavailable"),
            "year": title_info.get("year"),
            "url": f"https://www.imdb.com/title/{title_id}/",
        })

    TOP5_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(TOP5_FILE, "w", encoding="utf-8") as file:
        json.dump(top_5, file, indent=2, ensure_ascii=False)

    logging.info("Top five IMDb shows:")
    for show in top_5:
        logging.info(
            "%s. %s (%s) - %s",
            show["rank"],
            show["title"],
            show["year"] or "Year unavailable",
            show["url"],
        )


   
# DAG definition
with DAG(
    dag_id="imdb_tv_analysis_dag",
    description="Fetch popular IMDb TV shows and extract the top five",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
    },
    tags=["imdb", "api", "analysis"],
) as dag:


    fetch_data = PythonOperator(
        task_id="fetch_most_popular_tv",
        python_callable=fetch_imdb_data,
    )


    analyze_top_5 = PythonOperator(
        task_id="extract_top_5_shows",
        python_callable=extract_top_5,
    )


    fetch_data >> analyze_top_5
