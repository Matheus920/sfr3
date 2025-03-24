# Import our centralized logging setup
from logging_setup import get_logger
import uuid
import time
from extract.extract_general_ledger_accounts import get_general_ledger_accounts
from load.load_into_snowflake import (
    get_snowflake_connection,
    load_general_ledger_account_transactions_into_snowflake,
    load_general_ledger_accounts_into_snowflake,
    load_general_ledger_transactions_into_snowflake,
)
from transform.transform_general_ledger_transactions import (
    transform_general_ledger_transactions,
)
from transform.transform_general_ledger_accounts import (
    transform_general_ledger_accounts,
)
from transform.transform_account_participation import map_account_participation

# Get a configured logger for this module
logger = get_logger(__name__)

run_id = uuid.uuid4()
logger.info(f"Starting ETL process with run_id: {run_id}")

if __name__ == "__main__":
    start_time = time.time()
    # Read general ledger accounts data
    with open("data/general_ledger_accounts.json", "r") as file:
        general_ledger_json_data = file.read()
    general_ledger_accounts = get_general_ledger_accounts(general_ledger_json_data)
    # Flatten the accounts
    flattened_general_ledger_accounts = transform_general_ledger_accounts(
        general_ledger_accounts
    )

    # Get account IDs from transformed accounts
    general_ledger_accounts_ids = {
        account.id for account in flattened_general_ledger_accounts
    }

    # Read transaction data
    with open("data/general_ledger_transactions.json", "r") as file:
        general_ledger_transactions_json_data = file.read()

    # Map accounts to transactions using the dedicated function
    transactions_per_account, all_transactions = map_account_participation(
        general_ledger_accounts_ids, general_ledger_transactions_json_data
    )
    transformed_transactions = transform_general_ledger_transactions(all_transactions)

    # Load data into Snowflake
    logger.info("Starting Snowflake loading operations")
    with get_snowflake_connection() as snowflake_connection:
        load_general_ledger_accounts_into_snowflake(
            flattened_general_ledger_accounts, snowflake_connection, str(run_id)
        )
        logger.info("General ledger accounts loaded")

        load_general_ledger_transactions_into_snowflake(
            transformed_transactions, snowflake_connection, str(run_id)
        )
        logger.info("General ledger transactions loaded")

        load_general_ledger_account_transactions_into_snowflake(
            transactions_per_account, snowflake_connection, str(run_id)
        )
        logger.info("General ledger account-transaction relationships loaded")

        # The with statement will automatically commit the transaction when exiting the block
        logger.info("All data loaded successfully, transaction will be committed")

    execution_time = time.time() - start_time
    logger.info(
        f"ETL process completed successfully in {execution_time:.2f} seconds (Run ID: {run_id})"
    )
