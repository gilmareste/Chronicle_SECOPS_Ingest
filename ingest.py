"""Main file to ingest data into Chronicle."""

import json
import os
import sys
from typing import Any, Dict, List, Optional, Sequence, Union

from google.auth.transport import requests as Requests
from google.oauth2 import service_account

from common import env_constants
from common import utils

AUTHORIZATION_SCOPES = ["https://www.googleapis.com/auth/malachite-ingestion"]
CUSTOMER_ID = utils.get_env_var(env_constants.ENV_CHRONICLE_CUSTOMER_ID)
REGION = utils.get_env_var(
    env_constants.ENV_CHRONICLE_REGION, required=False, default="us"
)
SERVICE_ACCOUNT = utils.get_env_var(
    env_constants.ENV_CHRONICLE_SERVICE_ACCOUNT, is_secret=True
)

# Base URL for ingestion API.
INGESTION_API_BASE_URL = "malachiteingestion-pa.googleapis.com"

# Base URL for Reference list API
REFERENCE_LIST_API_BASE_URL = "backstory.googleapis.com"

# Threshold value in bytes for ingesting the logs to the Chronicle.
SIZE_THRESHOLD_BYTES = 950000

# Count of logs to check the batch size at once.
LOG_BATCH_SIZE = 100

SERVICE_ACCOUNT_DICT = utils.load_service_account(SERVICE_ACCOUNT, "Chronicle")


def initialize_http_session(
    service_account_json: Dict[Any, Any],
    scopes: Optional[Sequence[str]] = None,
) -> Requests.AuthorizedSession:
    """Initializes an authenticated session with Google Chronicle.

    Args:
        service_account_json (dict): Service Account JSON.
        scopes (Optional[Sequence[str]], optional): Required scopes. Defaults to None.

    Returns:
        Requests.AuthorizedSession: Authorized session object.
    """
    credentials = service_account.Credentials.from_service_account_info(
        service_account_json,
        scopes=scopes or AUTHORIZATION_SCOPES,
    )
    return Requests.AuthorizedSession(credentials)


def ingest(data: List[Any], log_type: str):
    """Prepare the chunk size of 0.95MB and send it to Chronicle.

    Args:
        data (List[Any]): Raw logs to send to Google Chronicle.
        log_type (str): Chronicle log type, for example: STIX
    """
    http_session = initialize_http_session(
        SERVICE_ACCOUNT_DICT, scopes=AUTHORIZATION_SCOPES
    )

    index = 0
    namespace = os.getenv(env_constants.ENV_CHRONICLE_NAMESPACE)

    parsed_data = [{"logText": json.dumps(i)} for i in data]

    body = {
        "customerId": CUSTOMER_ID,
        "logType": log_type,
        "entries": [],
    }
    if namespace:
        body["namespace"] = namespace

    while index < len(parsed_data):
        next_batch_of_logs = parsed_data[index: index + LOG_BATCH_SIZE]
        size_of_current_payload = sys.getsizeof(json.dumps(body))
        size_of_next_batch = sys.getsizeof(json.dumps(next_batch_of_logs))

        if size_of_next_batch >= SIZE_THRESHOLD_BYTES:
            print(
                "Size of next 100 logs to ingest is greater than 0.95MB. Hence,"
                " looping over each log separately."
            )
            size_of_next_log = sys.getsizeof(json.dumps(parsed_data[index]))
            if size_of_current_payload + size_of_next_log <= SIZE_THRESHOLD_BYTES:
                body["entries"].append(parsed_data[index])
                index += 1
                continue

        elif size_of_current_payload + size_of_next_batch <= SIZE_THRESHOLD_BYTES:
            print("Adding a batch of 100 logs to the Ingestion API payload.")
            body["entries"].extend(next_batch_of_logs)
            index += LOG_BATCH_SIZE
            continue

        _send_logs_to_chronicle(
            http_session,
            body,
            REGION,
        )
        body["entries"].clear()

    if body["entries"]:
        _send_logs_to_chronicle(http_session, body, REGION)


def _send_logs_to_chronicle(
    http_session: Requests.AuthorizedSession,
    body: Dict[str, List[Any]],
    region: str,
):
    """Sends unstructured log entries to the Chronicle backend for ingestion.

    Args:
        http_session (Requests.AuthorizedSession): Authorized session for HTTP requests.
        body (Dict[str, List[Any]]): JSON payload to send to Chronicle Ingestion API.
        region (str): Region of Ingestion API.

    Raises:
        RuntimeError: Raises if any error occured during log ingestion.
    """
    if region.lower() != "us":
        url = (
            f"https://{region.lower()}-{INGESTION_API_BASE_URL}/v2/unstructuredlogentries:batchCreate"
        )
    else:
        url = f"https://{INGESTION_API_BASE_URL}/v2/unstructuredlogentries:batchCreate"

    header = {"Content-Type": "application/json"}
    log_count = len(body["entries"])
    print(f"Attempting to push {log_count} log(s) to Chronicle.")

    response = http_session.request("POST", url, json=body, headers=header)

    try:
        response.raise_for_status()
        if not response.json():
            print(f"{log_count} log(s) pushed successfully to Chronicle.")
    except Exception as err:
        try:
            error_message = response.json().get("error", "Unknown error")
        except ValueError:
            error_message = "Failed to parse error response from Chronicle"
        raise RuntimeError(
            f"Error occurred while pushing logs to Chronicle. "
            f"Status code {response.status_code}. Reason: {error_message}"
        ) from err


def get_reference_list(list_name: str) -> Union[List[str], None]:
    """Get the reference list data from Chronicle.

    Args:
        list_name (str): Reference list name in Chronicle.

    Raises:
        RuntimeError: Raise error when list is not accessible.

    Returns:
        Union[List[str], None]: The contents of the list.
    """
    print(f"Fetching Reference list data for {list_name}.")
    http_session = initialize_http_session(
        SERVICE_ACCOUNT_DICT,
        scopes=["https://www.googleapis.com/auth/chronicle-backstory"],
    )
    if REGION.lower() != "us":
        url = (
            f"https://{REGION.lower()}-{REFERENCE_LIST_API_BASE_URL}/v2/lists/{list_name}"
        )
    else:
        url = f"https://{REFERENCE_LIST_API_BASE_URL}/v2/lists/{list_name}"

    header = {"Content-Type": "application/json"}
    response = http_session.request("GET", url, headers=header)
    try:
        response.raise_for_status()
        if response:
            list_content = response.json().get("lines", [])
            stripped_list_content = [s.strip() for s in list_content if s.strip()]
            return stripped_list_content
    except Exception as err:
        raise RuntimeError(
            f"Error occurred while fetching reference list {list_name}. "
            f"Status code: {response.status_code}. Reason: {err}"
        ) from err
