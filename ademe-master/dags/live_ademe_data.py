from __future__ import annotations
import os
import json
from urllib.parse import urlparse, parse_qs
import requests

from datetime import datetime, timedelta

from airflow.models.dag import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import logging
logger = logging.getLogger(__name__)


DATA_PATH = f"/opt/airflow/data/"
DOWNLOADED_FILES_PATH = os.path.join(DATA_PATH, "ademe-dpe-tertiaire")
URL_FILE = os.path.join(DATA_PATH, "api", "url.json")
RESULTS_FILE = os.path.join(DATA_PATH, "api", "results.json")


def check_environment_setup():
    logger.info("--" * 20)
    logger.info(f"[info logger] cwd: {os.getcwd()}")
    assert os.path.isfile(URL_FILE)
    assert os.path.isfile(RESULTS_FILE)
    logger.info(f"[info logger] URL_FILE: {URL_FILE}")
    logger.info(f"[info logger] RESULTS_FILE: {RESULTS_FILE}")
    logger.info("--" * 20)

def interrogate_api():
    """
    Interrogates the ADEME API using the specified URL and payload from a JSON file.

    - Reads the URL and payload from a JSON file defined by the constant `URL_FILE`.
    - Performs a GET request to the obtained URL with the given payload.
    - Saves the results to a JSON file defined by the constant `RESULTS_FILE`.

    Raises:
        AssertionError: If the URL file does not exist, or if the retrieved URL or payload is None.
        requests.exceptions.RequestException: If the GET request encounters an error.

    """
    # open url file
    with open(URL_FILE, encoding="utf-8") as file:
        url = json.load(file)
    assert url.get("url") is not None
    assert url.get("payload") is not None

    # make GET requests
    results = requests.get(url.get("url"), params=url.get("payload"), timeout=5)
    assert results.raise_for_status() is None
    data = results.json()

    # save results to file
    with open(RESULTS_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "live_ademe",
    default_args=default_args,
    description="Get ademe data",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:


    check_environment_setup = PythonOperator(
        task_id="check_environment_setup",
        python_callable=check_environment_setup,
    )

    interrogate_api = PythonOperator(
        task_id="interrogate_api",
        python_callable=interrogate_api,
    )

    check_environment_setup >> interrogate_api
