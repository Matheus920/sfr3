import datetime
import requests
import time
import json
from logging_setup import get_logger
from models.general_ledger_transactions import (
    GeneralLedgerTransaction,
    GeneralLedgerTransactions,
)
from typing import Optional
from pydantic import ValidationError

# Get configured logger for this module
logger = get_logger(__name__)


def transactions_json_search(
    json_transactions: list[dict], general_account_id: int
) -> str:
    """
    Filter transactions by general ledger account.

    Iterates through a list of transaction dictionaries, and for each transaction that has a
    "Journal" with "Lines", checks if any line contains a "GLAccount" with an "Id" matching
    the provided general_account_id. If a match is found, the transaction is added to the results.

    Parameters:
        json_transactions (list[dict]): A list of transaction dictionaries.
        general_account_id (int): The general ledger account ID to filter transactions by.

    Returns:
        str: A JSON string representing the list of filtered transactions.
    """
    result = []
    for json_transaction in json_transactions:
        if not json_transaction.get("Journal"):
            continue
        journal = json_transaction["Journal"]
        if not journal.get("Lines"):
            continue
        for line in journal["Lines"]:
            if not line.get("GLAccount"):
                continue
            account = line["GLAccount"]
            if account["Id"] == general_account_id:
                result.append(json_transaction)
                break

    return json.dumps(result)


def fetch_transactions_from_json(json_data: str, general_account_id: int) -> str:
    """
    Filter transactions from provided JSON data.

    Loads the given JSON string into a Python object and filters the transactions using the
    transactions_json_search function based on the provided general_account_id.

    Parameters:
        json_data (str): A JSON string containing transactions.
        general_account_id (int): The general ledger account ID to filter transactions by.

    Returns:
        str: A JSON string representing the filtered transactions.
    """
    logger.debug("Using provided JSON data")
    full_json_transactions = json.loads(json_data)
    return transactions_json_search(full_json_transactions, general_account_id)


def fetch_transactions_from_api(
    general_account_id: int, start_date: datetime.date, end_date: datetime.date
) -> str:
    """
    Fetch transactions from the Buildium API.

    Retrieves general ledger transactions for a specified general ledger account within a given
    date range from the Buildium API. Implements retry logic with exponential backoff if the
    API rate limit is exceeded.

    Parameters:
        general_account_id (int): The general ledger account ID to fetch transactions for.
        start_date (datetime.date): The start date for the transaction search.
        end_date (datetime.date): The end date for the transaction search.

    Returns:
        str: A JSON string containing the fetched transactions.

    Raises:
        ValueError: If start_date or end_date is not provided.
        Exception: If the maximum number of retries is exceeded.
        requests.RequestException: If an error occurs during the API request.
    """
    if not start_date or not end_date:
        raise ValueError("Start and end dates are required for API call")
    delay = 2
    maximum_retries = 5
    current_retries = 0
    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "glaccountids": [
            general_account_id
        ],  # although the API expects a list, I'm assuming that we need to pass one account id at a time as per the requirements
    }
    # If no JSON data provided, fetch from API
    while current_retries < maximum_retries:
        logger.debug("Reading general ledger transactions from Buildium API")
        response = requests.get(
            "https://api.buildium.com/v1/generalledger/transactions",
            headers={
                "x-buildium-client-id": "YOUR_CLIENT_ID",
                "x-buildium-client-secret": "YOUR_CLIENT_SECRET",
            },
            params=params,
        )
        if response.status_code == 429:
            logger.warning(
                f"Rate limit exceeded, waiting {delay} seconds before retrying"
            )
            time.sleep(delay)
            delay *= 2
            current_retries += 1
        else:
            # This will raise an HTTPError if the response was not 200 OK.
            response.raise_for_status()
            return response.text
    raise Exception("Max retries exceeded while fetching transactions from API")


def get_general_ledger_transactions(
    general_account_id: int,
    start_date: datetime.date = None,
    end_date: datetime.date = None,
    json_data: str = None,
) -> Optional[list[GeneralLedgerTransaction]]:
    """
    Retrieve and validate general ledger transactions.

    Depending on whether JSON data is provided, this function either filters transactions
    from the given JSON data or fetches them from the Buildium API using the specified date range.
    The retrieved JSON is then validated and converted into a list of GeneralLedgerTransaction objects.

    Parameters:
        general_account_id (int): The general ledger account ID to retrieve transactions for.
        start_date (datetime.date, optional): The start date for fetching transactions (required if json_data is not provided).
        end_date (datetime.date, optional): The end date for fetching transactions (required if json_data is not provided).
        json_data (str, optional): A JSON string containing transactions, used for testing instead of making an API call.

    Returns:
        Optional[list[GeneralLedgerTransaction]]: A list of validated general ledger transaction objects.

    Raises:
        ValueError: If general_account_id is not provided or if start_date/end_date are missing when needed.
        requests.RequestException: If an error occurs during the API request.
        ValidationError: If the JSON data fails validation.
        Exception: For any other unexpected errors during processing.
    """
    if not general_account_id:
        raise ValueError("General account ID is required")

    try:
        if json_data is not None:
            json_transactions = fetch_transactions_from_json(
                json_data, general_account_id
            )
        else:
            if not start_date or not end_date:
                raise ValueError("Start and end dates are required for API call")
            json_transactions = fetch_transactions_from_api(
                general_account_id, start_date, end_date
            )
    except requests.RequestException as e:
        logger.error("API request error occurred")
        raise e
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while reading GL Transactions for account id {general_account_id}"
        )
        raise e

    try:
        logger.debug("Validating transactions")
        result = GeneralLedgerTransactions.validate_json(json_transactions)
        logger.info(f"Read {len(result)} general ledger transactions for account ID {general_account_id}")
    except ValidationError as e:
        logger.error(f"Error validating transactions: {e}")
        raise e
    except Exception as e:
        logger.error("An unexpected error occurred while processing transactions")
        raise e

    return result
