from logging_setup import get_logger
from typing import List, Set
from models.general_ledger_account import GeneralLedgerAccount
from models.general_ledger_account_transactions import GeneralLedgerAccountTransactions
from models.general_ledger_transactions import GeneralLedgerTransaction
from extract.extract_general_ledger_transactions import get_general_ledger_transactions

# Get configured logger for this module
logger = get_logger(__name__)


def map_account_participation(
    general_ledger_accounts_ids: Set[int], general_ledger_transactions_json_data: str
) -> tuple[List[GeneralLedgerAccountTransactions], List[GeneralLedgerTransaction]]:
    logger.info(f"Starting account participation mapping for {len(general_ledger_accounts_ids)} accounts")
    """
    Map account participation in transactions.

    For each general ledger account ID provided, this function retrieves transactions from the given JSON data,
    maps the account to each transaction it appears in, and aggregates all unique transactions. The output is a tuple
    containing a list of account-to-transaction mappings and a list of the transactions themselves.

    Parameters:
        general_ledger_accounts_ids (Set[int]): A set of general ledger account IDs to map.
        general_ledger_transactions_json_data (str): A JSON string containing transaction data.

    Returns:
        tuple[List[GeneralLedgerAccountTransactions], List[GeneralLedgerTransaction]]:
            A tuple containing:
            - A list of GeneralLedgerAccountTransactions objects, each linking an account ID with a transaction ID.
            - A list of all unique GeneralLedgerTransaction objects retrieved from the JSON data.
    """
    transactions_per_account = []
    all_transactions = []

    for account_id in general_ledger_accounts_ids:
        logger.debug(f"Processing transactions for account ID: {account_id}")
        try:
            transactions = get_general_ledger_transactions(
                json_data=general_ledger_transactions_json_data,
                general_account_id=account_id,
            )
            
            if transactions:
                logger.debug(f"Found {len(transactions)} transactions for account ID: {account_id}")
            else:
                logger.debug(f"No transactions found for account ID: {account_id}")

            if transactions:
                all_transactions.extend(transactions)
                logger.debug(f"Added {len(transactions)} transactions to all_transactions")
                
                for transaction in transactions:
                    transactions_per_account.append(
                        GeneralLedgerAccountTransactions(
                            account_id=account_id, transaction_id=transaction.id
                        )
                    )
        except Exception as e:
            logger.error(f"Error processing transactions for account ID {account_id}: {str(e)}", exc_info=True)
            raise

    logger.info(f"Account participation mapping completed: {len(transactions_per_account)} account-transaction relationships mapped, {len(all_transactions)} transactions processed")
    return transactions_per_account, all_transactions
