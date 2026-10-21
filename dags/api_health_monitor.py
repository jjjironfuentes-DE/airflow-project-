from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from time import perf_counter
import logging
import os


from airflow import DAG # type: ignore
from airflow.exceptions import AirflowException # type: ignore
from airflow.providers.standard.operators.python import ( # type: ignore
    PythonOperator,
    BranchPythonOperator,
)
from airflow.utils.trigger_rule import TriggerRule # type: ignore




# Change this URL to the API you want to monitor.
#API_URL = "https://httpbin.org/status/200"
API_URL = "https://www.zelelearning.com/"
# A response must arrive within this many seconds.
REQUEST_TIMEOUT = 10


REPORT_DIR = "/opt/airflow/logs/reports"
REPORT_FILE = os.path.join(REPORT_DIR, "api_health_report.txt")




# TASK 1: Call the API and measure its health.
def check_api_health():
    request = Request(
        API_URL,
        headers={"User-Agent": "Airflow-API-Health-Monitor"},
        method="GET",
    )


    start_time = perf_counter()


    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            status_code = response.status


    except HTTPError as error:
        # HTTP errors such as 404 and 503 are responses too.
        status_code = error.code


    # Network errors and timeouts are allowed to raise exceptions.
    # Airflow will retry this task according to its retry settings.


    response_time_ms = round(
        (perf_counter() - start_time) * 1000, 2
    )


    result = {
        "url": API_URL,
        "status_code": status_code,
        "response_time_ms": response_time_ms,
        "checked_at": datetime.now().astimezone().isoformat(),
        "healthy": 200 <= status_code < 400,
    }


    logging.info("API health result: %s", result)


    return result




# TASK 2: Choose the next task based on the API result.
def choose_health_branch(ti):
    result = ti.xcom_pull(task_ids="check_api_health")


    if result["healthy"]:
        return "record_healthy"


    return "record_unhealthy"




# TASK 3A: Record a successful health check.
def record_healthy(ti):
    result = ti.xcom_pull(task_ids="check_api_health")


    logging.info(
        "API is HEALTHY | URL=%s | HTTP=%s | Response time=%s ms",
        result["url"],
        result["status_code"],
        result["response_time_ms"],
    )




# TASK 3B: Record an unhealthy API and mark the task as failed.
def record_unhealthy(ti):
    result = ti.xcom_pull(task_ids="check_api_health")


    logging.error(
        "API is UNHEALTHY | URL=%s | HTTP=%s | Response time=%s ms",
        result["url"],
        result["status_code"],
        result["response_time_ms"],
    )


    raise AirflowException(
        f"API health check failed with HTTP status "
        f"{result['status_code']}"
    )




# TASK 4: Write a report regardless of the health outcome.
def write_health_report(ti):
    result = ti.xcom_pull(task_ids="check_api_health")


    if not result:
        raise AirflowException("No health-check result was available.")


    os.makedirs(REPORT_DIR, exist_ok=True)


    report = f"""
API HEALTH MONITOR REPORT
=========================


URL: {result['url']}
Checked at: {result['checked_at']}
HTTP status: {result['status_code']}
Response time: {result['response_time_ms']} ms
Health status: {"HEALTHY" if result['healthy'] else "UNHEALTHY"}
"""


    with open(REPORT_FILE, "w", encoding="utf-8") as file:
        file.write(report.strip() + "\n")


    logging.info("Health report written to %s", REPORT_FILE)
    logging.info("%s", report)




with DAG(
    dag_id="api_health_monitor",
    description="Monitor API availability and response time",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
        "execution_timeout": timedelta(seconds=20),
    },
    tags=["monitoring", "api", "intermediate"],
) as dag:


    check_api = PythonOperator(
        task_id="check_api_health",
        python_callable=check_api_health,
    )


    branch = BranchPythonOperator(
        task_id="choose_health_branch",
        python_callable=choose_health_branch,
    )


    healthy = PythonOperator(
        task_id="record_healthy",
        python_callable=record_healthy,
    )


    unhealthy = PythonOperator(
        task_id="record_unhealthy",
        python_callable=record_unhealthy,
    )


    report = PythonOperator(
        task_id="write_health_report",
        python_callable=write_health_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )


    check_api >> branch
    branch >> [healthy, unhealthy]
    [healthy, unhealthy] >> report
