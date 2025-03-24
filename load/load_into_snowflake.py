import datetime
import json
import snowflake.connector
from models.general_ledger_account import GeneralLedgerAccountFlattened
from pydantic import BaseModel
import csv
import os
from logging_setup import get_logger

from models.general_ledger_account_transactions import GeneralLedgerAccountTransactions
from models.general_ledger_transactions import GeneralLedgerTransactionTransformed

# Get configured logger for this module
logger = get_logger(__name__)


def preprocess_row(row: dict, run_id: str) -> dict:
    """
    Preprocess a dictionary row by adding metadata and converting nested values.

    Adds the run_id and current timestamp (inserted_at) to the row, and converts any
    dict or list values to JSON strings.

    Parameters:
        row (dict): The input dictionary representing a row of data.
        run_id (str): A unique identifier for the current run.

    Returns:
        dict: The processed row with added metadata and JSON string conversion for dict and list values.
    """
    row["run_id"] = run_id
    row["inserted_at"] = datetime.datetime.now().isoformat()
    return {
        key: (json.dumps(value) if isinstance(value, (dict, list)) else value)
        for key, value in row.items()
    }


def export_data_to_csv(data: list[BaseModel], file_name: str, run_id: str) -> None:
    """
    Export a list of Pydantic model instances to a CSV file with additional metadata.

    Writes the model data to a CSV file in the /tmp/ directory, appending metadata
    (run_id and inserted_at) to each row. This file can later be used for staging data in Snowflake.

    Parameters:
        data (List[BaseModel]): A list of Pydantic model instances to export.
        file_name (str): The name of the CSV file to create (saved in /tmp/).
        run_id (str): A unique identifier for the current run, added to each row.

    Returns:
        None
    """
    logger.info(f"Exporting {len(data)} records to CSV file: {file_name}")
    try:
        with open("/tmp/" + file_name, "w", newline="") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=list(data[0].model_dump().keys()) + ["run_id", "inserted_at"],
                doublequote=False,
                escapechar="\\",
            )
            writer.writeheader()
            for row in data:
                row_without_metadata = row.model_dump()
                treated_row = preprocess_row(row_without_metadata, run_id)
                writer.writerow(treated_row)
        # At this point we would potentially store this CSV into a cloud storage bucket to maintain a record of the data that was loaded into Snowflake
        logger.info(f"Successfully exported data to /tmp/{file_name}")
    except Exception as e:
        logger.error(f"Failed to export data to CSV file: {str(e)}", exc_info=True)
        raise


def stage_file_in_snowflake(
    file_name: str,
    table_name: str,
    snowflake_connection: snowflake.connector.SnowflakeConnection,
) -> None:
    """
    Stage a CSV file in a Snowflake staging table.

    Uploads the CSV file located in /tmp/ to the specified staging table in Snowflake,
    then uses a COPY command to load the file data into the table.

    Parameters:
        file_name (str): The name of the CSV file (located in /tmp/).
        table_name (str): The name of the target staging table.
        snowflake_connection (snowflake.connector.SnowflakeConnection): An active connection to Snowflake.

    Returns:
        None
    """
    logger.info(f"Staging file {file_name} into Snowflake table {table_name}")
    try:
        with open("/tmp/" + file_name, "rb") as file:
            cursor = snowflake_connection.cursor()
            logger.debug("Setting schema to GENERAL_LEDGER_STAGING")
            cursor.execute("USE SCHEMA GENERAL_LEDGER_STAGING")
            
            logger.debug(f"Uploading file to Snowflake internal stage @%{table_name}")
            cursor.execute(f"PUT file://{file.name} @%{table_name} OVERWRITE = TRUE")
            
            logger.debug(f"Copying data from staged file into {table_name} table")
            cursor.execute(
                f"copy into {table_name} from @%{table_name}/{file_name}.gz file_format = (type = csv field_optionally_enclosed_by = '\"' skip_header = 1 escape='\\\\') on_error = 'abort_statement'"
            )
            
            # Get number of rows loaded
            rows_loaded = cursor.fetchone()[0] if cursor.rowcount > 0 else 0
            logger.info(f"Successfully loaded {rows_loaded} rows into staging table {table_name}")
    except Exception as e:
        logger.error(f"Failed to stage file in Snowflake: {str(e)}", exc_info=True)
        raise


def merge_staging_table_into_target_table(
    staging_table: str,
    target_table: str,
    columns: list[str],
    snowflake_connection: snowflake.connector.SnowflakeConnection,
    run_id: str,
    id_matching_columns: list[str] = ["id"],
) -> None:
    """
    Merge data from a staging table into a target table in Snowflake.

    Performs a MERGE operation in Snowflake by matching records on the specified columns.
    Only records from the staging table with the provided run_id are considered. When a record
    does not exist in the target table, it is inserted.

    Parameters:
        staging_table (str): The name of the staging table containing the data.
        target_table (str): The name of the target table for merging data.
        columns (List[str]): A list of column names to include in the INSERT clause.
        snowflake_connection (snowflake.connector.SnowflakeConnection): An active connection to Snowflake.
        run_id (str): The run identifier used to filter staging table data.
        id_matching_columns (List[str], optional): Columns used for matching records between source and target.
            Defaults to ["id"].

    Returns:
        None
    """
    logger.info(f"Merging data from {staging_table} into {target_table} for run_id: {run_id}")
    try:
        cursor = snowflake_connection.cursor()
        id_matching = " AND ".join(
            [f"target.{column} = source.{column}" for column in id_matching_columns]
        )
        logger.debug(f"Using match condition: {id_matching}")
        
        merge_query = f"MERGE INTO {target_table} AS target USING (SELECT * FROM {staging_table} WHERE run_id = %s) AS source ON {id_matching} WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({', '.join(columns)})"
        logger.debug(f"Executing merge query: {merge_query}")
        
        cursor.execute(merge_query, (run_id,))
        
        # Get statistics of the merge operation
        rows_inserted = cursor.rowcount
        logger.info(f"Merge completed: {rows_inserted} new rows inserted into {target_table}")
        
        # We can optionally delete records from the staging table after the merge, but this assumes
        # that a background process is responsible for cleaning up the staging table after a while, meaning that historical data might be available in the staging table for a while
        # for debugging purposes.
    except Exception as e:
        logger.error(f"Failed to merge data into target table: {str(e)}", exc_info=True) 
        raise


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """
    Establish a connection to Snowflake.

    Creates and returns a Snowflake connection using environment variables for configuration.
    Autocommit is disabled.

    Returns:
        snowflake.connector.SnowflakeConnection: An active connection to Snowflake.
    """
    logger.info("Establishing connection to Snowflake")
    logger.debug("TEST DEBUG LOG - This is a test message to verify DEBUG level logs are visible")
    try:
        connection = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            autocommit=False,
        )
        logger.info("Successfully connected to Snowflake")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}", exc_info=True)
        raise


def load_general_ledger_accounts_into_snowflake(
    general_ledger_accounts: list[GeneralLedgerAccountFlattened],
    snowflake_connection: snowflake.connector.SnowflakeConnection,
    run_id: str,
) -> None:
    """
    Load general ledger accounts into Snowflake.

    Exports a list of flattened general ledger account objects to a CSV file, stages the file in Snowflake,
    and merges the staged data into the target general ledger account table.

    Parameters:
        general_ledger_accounts (List[GeneralLedgerAccountFlattened]): A list of flattened general ledger account objects.
        snowflake_connection (snowflake.connector.SnowflakeConnection): An active connection to Snowflake.
        run_id (str): A unique identifier for the current run, used for file naming and filtering staging data.

    Returns:
        None
    """
    logger.info(f"Loading {len(general_ledger_accounts)} general ledger accounts into Snowflake")
    try:
        file_name = f"general_ledger_accounts_{run_id}.csv"
        export_data_to_csv(general_ledger_accounts, file_name, run_id)
        stage_file_in_snowflake(file_name, "account", snowflake_connection)
        merge_staging_table_into_target_table(
            "general_ledger_staging.account",
            "general_ledger.account",
            general_ledger_accounts[0].model_dump().keys(),
            snowflake_connection,
            run_id,
        )
        logger.info("General ledger accounts successfully staged for final commit")
    except Exception as e:
        logger.error(f"Failed to load general ledger accounts: {str(e)}", exc_info=True)
        raise


def load_general_ledger_transactions_into_snowflake(
    general_ledger_transactions: list[GeneralLedgerTransactionTransformed],
    snowflake_connection: snowflake.connector.SnowflakeConnection,
    run_id: str,
) -> None:
    """
    Load general ledger transactions into Snowflake.

    Exports a list of transformed general ledger transaction objects to a CSV file, stages the file in Snowflake,
    and merges the staged data into the target general ledger transaction table.

    Parameters:
        general_ledger_transactions (List[GeneralLedgerTransactionTransformed]): A list of transformed general ledger transaction objects.
        snowflake_connection (snowflake.connector.SnowflakeConnection): An active connection to Snowflake.
        run_id (str): A unique identifier for the current run, used for file naming and filtering staging data.

    Returns:
        None
    """
    logger.info(f"Loading {len(general_ledger_transactions)} general ledger transactions into Snowflake")
    try:
        file_name = f"general_ledger_transactions_{run_id}.csv"
        export_data_to_csv(general_ledger_transactions, file_name, run_id)
        stage_file_in_snowflake(file_name, "transaction", snowflake_connection)
        merge_staging_table_into_target_table(
            "general_ledger_staging.transaction",
            "general_ledger.transaction",
            general_ledger_transactions[0].model_dump().keys(),
            snowflake_connection,
            run_id,
        )
        logger.info("General ledger transactions successfully staged for final commit")
    except Exception as e:
        logger.error(f"Failed to load general ledger transactions: {str(e)}", exc_info=True)
        raise


def load_general_ledger_account_transactions_into_snowflake(
    general_ledger_account_transactions: list[GeneralLedgerAccountTransactions],
    snowflake_connection: snowflake.connector.SnowflakeConnection,
    run_id: str,
) -> None:
    """
    Load general ledger account transactions into Snowflake.

    Exports a list of account-to-transaction mapping objects to a CSV file, stages the file in Snowflake,
    and merges the staged data into the target general ledger account transactions table using specified matching columns.

    Parameters:
        general_ledger_account_transactions (List[GeneralLedgerAccountTransactions]): A list of account-to-transaction mapping objects.
        snowflake_connection (snowflake.connector.SnowflakeConnection): An active connection to Snowflake.
        run_id (str): A unique identifier for the current run, used for file naming and filtering staging data.

    Returns:
        None
    """
    logger.info(f"Loading {len(general_ledger_account_transactions)} general ledger account-transaction relationships into Snowflake")
    try:
        file_name = f"general_ledger_account_transactions_{run_id}.csv"
        export_data_to_csv(general_ledger_account_transactions, file_name, run_id)
        stage_file_in_snowflake(file_name, "account_transactions", snowflake_connection)
        merge_staging_table_into_target_table(
            "general_ledger_staging.account_transactions",
            "general_ledger.account_transactions",
            general_ledger_account_transactions[0].model_dump().keys(),
            snowflake_connection,
            run_id,
            id_matching_columns=["account_id", "transaction_id"],
        )
        logger.info("General ledger account-transaction relationships successfully staged for final commit")
    except Exception as e:
        logger.error(f"Failed to load general ledger account-transaction relationships: {str(e)}", exc_info=True)
        raise
