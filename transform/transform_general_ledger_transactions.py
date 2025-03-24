from logging_setup import get_logger
from models.general_ledger_transactions import (
    GeneralLedgerTransaction,
    GeneralLedgerTransactionTransformed,
)

# Get configured logger for this module
logger = get_logger(__name__)


def transform_general_ledger_transactions(
    transactions: list[GeneralLedgerTransaction],
) -> list[GeneralLedgerTransactionTransformed]:
    logger.info(f"Starting transformation of {len(transactions)} general ledger transactions")
    """
    Transform and flatten general ledger transactions.

    Processes a list of GeneralLedgerTransaction objects by removing duplicates (based on the transaction ID)
    and flattening nested journal data. For each unique transaction, the function excludes the 'journal' field,
    extracts the journal memo, and transforms each journal line by excluding the 'gl_account' field and adding the
    corresponding general ledger account ID. The transformed data is then used to create a
    GeneralLedgerTransactionTransformed object.

    Parameters:
        transactions (list[GeneralLedgerTransaction]): A list of general ledger transaction objects to transform.

    Returns:
        list[GeneralLedgerTransactionTransformed]: A list of transformed and flattened general ledger transaction objects.
    """
    result = []
    stored_ids = set()
    duplicates_found = 0

    for transaction in transactions:
        if transaction.id in stored_ids:
            logger.debug(f"Skipping duplicate transaction ID: {transaction.id}")
            duplicates_found += 1
            continue
        
        logger.debug(f"Processing transaction ID: {transaction.id}")
        stored_ids.add(transaction.id)
        try:
            transformed_transaction = transaction.model_dump(exclude={"journal"})
            transformed_transaction["journal_memo"] = transaction.journal.memo
            transformed_transaction["lines"] = []
            
            logger.debug(f"Transaction {transaction.id} has {len(transaction.journal.lines)} journal lines")
            
            for line in transaction.journal.lines:
                base_line = line.model_dump(exclude={"gl_account"})
                base_line["general_ledger_account_id"] = line.gl_account.id
                transformed_transaction["lines"].append(base_line)
                
            result.append(GeneralLedgerTransactionTransformed(**transformed_transaction))
        except Exception as e:
            logger.error(f"Error transforming transaction ID {transaction.id}: {str(e)}", exc_info=True)
            raise

    logger.info(f"Transformation completed: {len(result)} unique transactions processed, {duplicates_found} duplicates skipped")
    return result
