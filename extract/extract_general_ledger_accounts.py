import requests
from logging_setup import get_logger
from models.general_ledger_account import GeneralLedgerAccount, GeneralLedgerAccounts
from pydantic import ValidationError

# Get configured logger for this module
logger = get_logger(__name__)

def get_general_ledger_accounts(json_data: str = None) -> list[GeneralLedgerAccount]:
    """
    Retrieve and validate general ledger accounts.

    This function obtains general ledger account data either from a provided JSON string
    (useful for testing) or by fetching it from the Buildium API. The JSON data is then
    validated and converted into a list of GeneralLedgerAccount objects.

    Parameters:
        json_data (str, optional): A JSON string representing general ledger accounts.
            If provided, this data is used directly. Otherwise, the function fetches the data
            from the Buildium API using preset client credentials.

    Returns:
        list[GeneralLedgerAccount]: A list of validated general ledger account objects.

    Raises:
        requests.RequestException: If an error occurs during the API request.
        ValidationError: If the JSON data fails validation.
        Exception: For any other unexpected errors during data retrieval or processing.
    """
    result = []

    try:
        if json_data is not None:
            # Direct JSON data provided, use it directly (for testing)
            logger.debug('Using provided JSON data')
            json_accounts = json_data
        else:
            # If no JSON data provided, fetch from API
            logger.debug('Reading general ledger accounts from Buildium API')
            response = requests.get(
                'https://api.buildium.com/v1/glaccounts',
                headers={
                    'x-buildium-client-id': 'YOUR_CLIENT_ID',
                    'x-buildium-client-secret': 'YOUR_CLIENT_SECRET'
                }
            )
            # This will raise an HTTPError if the response was not 200 OK.
            response.raise_for_status()
            json_accounts = response.text
    except requests.RequestException as e:
        logger.error("API request error occurred")
        raise e
    except Exception as e:
        logger.error("An unexpected error occurred while reading GL accounts")
        raise e

    try:
        logger.debug(f"Validating accounts")
        result = GeneralLedgerAccounts.validate_json(json_accounts)
        logger.info(f"Read {len(result)} general ledger accounts")
    except ValidationError as e:
        logger.error(f"Error validating accounts: {e}")
        raise e
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing accounts")
        raise e

    return result
