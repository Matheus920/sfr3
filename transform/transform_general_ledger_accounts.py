from logging_setup import get_logger
from models.general_ledger_account import (
    GeneralLedgerAccount,
    GeneralLedgerAccountFlattened,
)

# Get configured logger for this module
logger = get_logger(__name__)


def transform_general_ledger_accounts(
    accounts: list[GeneralLedgerAccount],
) -> list[GeneralLedgerAccountFlattened]:
    logger.info(f"Starting transformation of {len(accounts)} general ledger accounts")
    """
    Flatten a list of GeneralLedgerAccount objects, including their sub-accounts.

    This function converts each GeneralLedgerAccount into a flattened representation by
    dumping its data (excluding the "sub_accounts" field) into a GeneralLedgerAccountFlattened
    object. It then processes each sub-account (assuming only one level of sub-accounts) in the
    same manner and adds them to the resulting list.

    Parameters:
        accounts (list[GeneralLedgerAccount]): A list of general ledger account objects to transform.

    Returns:
        list[GeneralLedgerAccountFlattened]: A flattened list of general ledger accounts including sub-accounts.
    """
    transformed = []
    sub_account_count = 0

    for account in accounts:
        logger.debug(f"Transforming account: {account.id} - {account.name}")
        transformed.append(
            GeneralLedgerAccountFlattened(
                **account.model_dump(exclude={"sub_accounts"})
            )
        )
        # This assumes that an account can only have one level of sub-accounts.
        # If there are more levels, a recursive function would be needed.
        if account.sub_accounts:
            logger.debug(f"Account {account.id} has {len(account.sub_accounts)} sub-accounts")
        
        for sub_account in account.sub_accounts:
            logger.debug(f"Transforming sub-account: {sub_account.id} - {sub_account.name}")
            sub_account_count += 1
            transformed.append(
                GeneralLedgerAccountFlattened(
                    **sub_account.model_dump(exclude={"sub_accounts"})
                )
            )

    logger.info(f"Transformation completed: flattened {len(accounts)} accounts and {sub_account_count} sub-accounts into {len(transformed)} total accounts")
    return transformed
