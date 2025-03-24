import pytest
import datetime
import json
import os
from unittest.mock import Mock, patch, mock_open, call
import snowflake.connector
from io import StringIO

from models.general_ledger_account import GeneralLedgerAccountFlattened
from models.general_ledger_transactions import GeneralLedgerTransactionTransformed, UnitAgreement, PaymentDetail, DepositDetails, JournalLineTransformed, AccountingEntity, Unit
from models.general_ledger_account_transactions import GeneralLedgerAccountTransactions
from load.load_into_snowflake import (
    preprocess_row,
    export_data_to_csv,
    stage_file_in_snowflake,
    merge_staging_table_into_target_table,
    get_snowflake_connection,
    load_general_ledger_accounts_into_snowflake,
    load_general_ledger_transactions_into_snowflake,
    load_general_ledger_account_transactions_into_snowflake,
)


# Helper function to create a test GL account
def create_test_gl_account(account_id=1001):
    return GeneralLedgerAccountFlattened(
        Id=account_id,
        Name="Test Account",
        Type="Income",
        SubType="Income",
        IsDefaultGLAccount=True,
        DefaultAccountName="Test Income",
        IsContraAccount=False,
        IsBankAccount=False,
        CashFlowClassification="OperatingActivities",
        ExcludeFromCashBalances=False,
        IsActive=True
    )


# Helper function to create a test transaction
def create_test_transaction(transaction_id=2001):
    # Create a Unit for the accounting entity
    unit = Unit(
        Id=500,
        Href="https://api.example.com/units/500"
    )
    
    # Create an accounting entity
    accounting_entity = AccountingEntity(
        Id=200,
        AccountingEntityType="Rental",
        Href="https://api.example.com/rentals/200",
        Unit=unit
    )
    
    # Create a journal line transformed
    journal_line = JournalLineTransformed(
        Amount=100.00,
        IsCashPosting=False,
        ReferenceNumber="REF123",
        Memo="Test Transaction",
        general_ledger_account_id=1001,
        AccountingEntity=accounting_entity
    )
    
    # Create a unit agreement
    unit_agreement = UnitAgreement(
        Id=300,
        Type="Lease",
        Href="https://api.example.com/leases/300"
    )
    
    # Create payment details
    payment_detail = PaymentDetail(
        PaymentMethod="Check",
        Payee="John Doe",
        IsInternalTransaction=False,
        InternalTransactionStatus=None
    )
    
    # Create deposit details
    deposit_details = DepositDetails(
        BankGLAccountId=None,
        PaymentTransactions=[]
    )
    
    # Create the transaction
    transaction = GeneralLedgerTransactionTransformed(
        Id=transaction_id,
        Date=datetime.date(2024, 3, 15),
        TransactionType="Charge",
        TotalAmount=100.00,
        CheckNumber="12345",
        UnitAgreement=unit_agreement,
        UnitId=400,
        UnitNumber="Unit400",
        PaymentDetail=payment_detail,
        DepositDetails=deposit_details,
        journal_memo="Test Journal Memo",
        lines=[journal_line],
        LastUpdatedDateTime=datetime.datetime(2024, 3, 15, 12, 0, 0)
    )
    
    return transaction


# Helper function to create test account transaction
def create_test_account_transaction(account_id=1001, transaction_id=2001):
    return GeneralLedgerAccountTransactions(
        account_id=account_id,
        transaction_id=transaction_id
    )


class TestPreprocessRow:
    def test_preprocess_row_with_basic_data(self):
        """Test preprocessing a row with basic data types."""
        # Setup
        run_id = "test-run-123"
        row = {"id": 1, "name": "Test Account"}
        
        # Execute
        result = preprocess_row(row, run_id)
        
        # Verify
        assert result["id"] == 1
        assert result["name"] == "Test Account"
        assert result["run_id"] == run_id
        assert "inserted_at" in result
        
    def test_preprocess_row_with_complex_data(self):
        """Test preprocessing a row with complex data types (dict, list)."""
        # Setup
        run_id = "test-run-123"
        row = {
            "id": 1, 
            "metadata": {"type": "account"}, 
            "tags": ["test", "account"]
        }
        
        # Execute
        result = preprocess_row(row, run_id)
        
        # Verify
        assert result["id"] == 1
        assert result["metadata"] == json.dumps({"type": "account"})
        assert result["tags"] == json.dumps(["test", "account"])
        assert result["run_id"] == run_id
        assert "inserted_at" in result


class TestExportDataToCSV:
    @patch("builtins.open", new_callable=mock_open)
    def test_export_data_to_csv(self, mock_file):
        """Test exporting data to CSV."""
        # Setup
        run_id = "test-run-123"
        data = [create_test_gl_account()]
        file_name = "test_accounts.csv"
        
        # Mock the preprocess_row function to provide a predictable result
        with patch("load.load_into_snowflake.preprocess_row") as mock_preprocess:
            mock_preprocess.return_value = {
                "id": 1001,
                "name": "Test Account",
                "run_id": run_id,
                "inserted_at": "2024-03-23T10:00:00.000000"
            }
            
            # Execute
            export_data_to_csv(data, file_name, run_id)
            
            # Verify
            mock_file.assert_called_once_with("/tmp/test_accounts.csv", "w", newline="")
            handle = mock_file()
            
            # Check that the writer was used (the file was written to)
            assert handle.write.call_count > 0
            
            # Check that preprocess_row was called
            mock_preprocess.assert_called()


class TestStageFileInSnowflake:
    def test_stage_file_in_snowflake(self):
        """Test staging a file in Snowflake."""
        # Setup
        mock_cursor = Mock()
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Configure the mock cursor to return sample row count on fetchone()
        mock_cursor.rowcount = 1
        mock_cursor.fetchone.return_value = [10]  # Return 10 rows loaded
        
        # Create a context manager mock
        cm_mock = Mock()
        mock_file = Mock()
        mock_file.name = "/tmp/test_file.csv"
        cm_mock.__enter__ = Mock(return_value=mock_file)
        cm_mock.__exit__ = Mock(return_value=None)
        
        # Patch the open function
        with patch("builtins.open", return_value=cm_mock):
            # Execute
            stage_file_in_snowflake("test_file.csv", "test_table", mock_conn)
            
            # Verify
            assert mock_conn.cursor.called
            assert mock_cursor.execute.call_count == 3
            
            # Check that the correct SQL commands were executed
            mock_cursor.execute.assert_any_call("USE SCHEMA GENERAL_LEDGER_STAGING")
            mock_cursor.execute.assert_any_call("PUT file:///tmp/test_file.csv @%test_table OVERWRITE = TRUE")
            
            # The final execute contains the copy command which we'll check partially
            copy_command_call = mock_cursor.execute.call_args_list[2]
            assert "copy into test_table" in copy_command_call[0][0].lower()
            
            # Verify that fetchone was called to get row count
            mock_cursor.fetchone.assert_called_once()


class TestMergeStagingTableIntoTargetTable:
    def test_merge_staging_table_into_target_table(self):
        """Test merging a staging table into a target table."""
        # Setup
        mock_cursor = Mock()
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        staging_table = "staging.test"
        target_table = "prod.test"
        columns = ["id", "name", "value"]
        run_id = "test-run-123"
        
        # Execute
        merge_staging_table_into_target_table(
            staging_table, 
            target_table, 
            columns, 
            mock_conn, 
            run_id
        )
        
        # Verify
        assert mock_conn.cursor.called
        assert mock_cursor.execute.call_count == 1
        
        # Check that the MERGE SQL command was executed with the right parameters
        merge_command_call = mock_cursor.execute.call_args
        assert "MERGE INTO" in merge_command_call[0][0]
        assert "target.id = source.id" in merge_command_call[0][0]
        assert run_id in merge_command_call[0][1]
        
    def test_merge_staging_table_with_compound_key(self):
        """Test merging a staging table with a compound primary key."""
        # Setup
        mock_cursor = Mock()
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        staging_table = "staging.test"
        target_table = "prod.test"
        columns = ["account_id", "transaction_id", "amount"]
        run_id = "test-run-123"
        id_matching_columns = ["account_id", "transaction_id"]
        
        # Execute
        merge_staging_table_into_target_table(
            staging_table, 
            target_table, 
            columns, 
            mock_conn, 
            run_id,
            id_matching_columns
        )
        
        # Verify
        assert mock_conn.cursor.called
        assert mock_cursor.execute.call_count == 1
        
        # Check that the MERGE SQL command was executed with the right parameters
        merge_command_call = mock_cursor.execute.call_args
        assert "MERGE INTO" in merge_command_call[0][0]
        
        # Should contain both parts of the compound key
        assert "target.account_id = source.account_id" in merge_command_call[0][0]
        assert "target.transaction_id = source.transaction_id" in merge_command_call[0][0]
        assert run_id in merge_command_call[0][1]


class TestGetSnowflakeConnection:
    @patch.dict(os.environ, {
        "SNOWFLAKE_USER": "test_user",
        "SNOWFLAKE_PASSWORD": "test_password",
        "SNOWFLAKE_ACCOUNT": "test_account",
        "SNOWFLAKE_WAREHOUSE": "test_warehouse",
        "SNOWFLAKE_DATABASE": "test_database"
    })
    @patch("snowflake.connector.connect")
    def test_get_snowflake_connection(self, mock_connect):
        """Test getting a Snowflake connection."""
        # Setup
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        # Execute
        result = get_snowflake_connection()
        
        # Verify
        assert result == mock_conn
        mock_connect.assert_called_once_with(
            user="test_user",
            password="test_password",
            account="test_account",
            warehouse="test_warehouse",
            database="test_database",
            autocommit=False
        )


class TestLoadGeneralLedgerAccountsIntoSnowflake:
    def test_load_general_ledger_accounts_into_snowflake(self, monkeypatch):
        """Test loading GL accounts into Snowflake."""
        # Setup
        mock_conn = Mock()
        accounts = [create_test_gl_account(1001), create_test_gl_account(1002)]
        run_id = "test-run-123"
        
        # Mock the helper functions
        mock_export = Mock()
        mock_stage = Mock()
        mock_merge = Mock()
        
        monkeypatch.setattr("load.load_into_snowflake.export_data_to_csv", mock_export)
        monkeypatch.setattr("load.load_into_snowflake.stage_file_in_snowflake", mock_stage)
        monkeypatch.setattr("load.load_into_snowflake.merge_staging_table_into_target_table", mock_merge)
        
        # Execute
        load_general_ledger_accounts_into_snowflake(accounts, mock_conn, run_id)
        
        # Verify
        # Check that export was called with the right parameters
        mock_export.assert_called_once()
        export_args = mock_export.call_args
        assert export_args[0][0] == accounts
        assert "general_ledger_accounts" in export_args[0][1]
        assert export_args[0][2] == run_id
        
        # Check that stage was called with the right parameters
        mock_stage.assert_called_once()
        stage_args = mock_stage.call_args
        assert "general_ledger_accounts" in stage_args[0][0]
        assert stage_args[0][1] == "account"
        assert stage_args[0][2] == mock_conn
        
        # Check that merge was called with the right parameters
        mock_merge.assert_called_once()
        merge_args = mock_merge.call_args
        assert merge_args[0][0] == "general_ledger_staging.account"
        assert merge_args[0][1] == "general_ledger.account"
        assert merge_args[0][3] == mock_conn
        assert merge_args[0][4] == run_id


class TestLoadGeneralLedgerTransactionsIntoSnowflake:
    def test_load_general_ledger_transactions_into_snowflake(self, monkeypatch):
        """Test loading GL transactions into Snowflake."""
        # Setup
        mock_conn = Mock()
        transactions = [create_test_transaction(2001), create_test_transaction(2002)]
        run_id = "test-run-123"
        
        # Mock the helper functions
        mock_export = Mock()
        mock_stage = Mock()
        mock_merge = Mock()
        
        monkeypatch.setattr("load.load_into_snowflake.export_data_to_csv", mock_export)
        monkeypatch.setattr("load.load_into_snowflake.stage_file_in_snowflake", mock_stage)
        monkeypatch.setattr("load.load_into_snowflake.merge_staging_table_into_target_table", mock_merge)
        
        # Execute
        load_general_ledger_transactions_into_snowflake(transactions, mock_conn, run_id)
        
        # Verify
        # Check that export was called with the right parameters
        mock_export.assert_called_once()
        export_args = mock_export.call_args
        assert export_args[0][0] == transactions
        assert "general_ledger_transactions" in export_args[0][1]
        assert export_args[0][2] == run_id
        
        # Check that stage was called with the right parameters
        mock_stage.assert_called_once()
        stage_args = mock_stage.call_args
        assert "general_ledger_transactions" in stage_args[0][0]
        assert stage_args[0][1] == "transaction"
        assert stage_args[0][2] == mock_conn
        
        # Check that merge was called with the right parameters
        mock_merge.assert_called_once()
        merge_args = mock_merge.call_args
        assert merge_args[0][0] == "general_ledger_staging.transaction"
        assert merge_args[0][1] == "general_ledger.transaction"
        assert merge_args[0][3] == mock_conn
        assert merge_args[0][4] == run_id


class TestLoadGeneralLedgerAccountTransactionsIntoSnowflake:
    def test_load_general_ledger_account_transactions_into_snowflake(self, monkeypatch):
        """Test loading GL account transactions into Snowflake."""
        # Setup
        mock_conn = Mock()
        account_transactions = [
            create_test_account_transaction(1001, 2001),
            create_test_account_transaction(1002, 2002)
        ]
        run_id = "test-run-123"
        
        # Mock the helper functions
        mock_export = Mock()
        mock_stage = Mock()
        mock_merge = Mock()
        
        monkeypatch.setattr("load.load_into_snowflake.export_data_to_csv", mock_export)
        monkeypatch.setattr("load.load_into_snowflake.stage_file_in_snowflake", mock_stage)
        monkeypatch.setattr("load.load_into_snowflake.merge_staging_table_into_target_table", mock_merge)
        
        # Execute
        load_general_ledger_account_transactions_into_snowflake(account_transactions, mock_conn, run_id)
        
        # Verify
        # Check that export was called with the right parameters
        mock_export.assert_called_once()
        export_args = mock_export.call_args
        assert export_args[0][0] == account_transactions
        assert "general_ledger_account_transactions" in export_args[0][1]
        assert export_args[0][2] == run_id
        
        # Check that stage was called with the right parameters
        mock_stage.assert_called_once()
        stage_args = mock_stage.call_args
        assert "general_ledger_account_transactions" in stage_args[0][0]
        assert stage_args[0][1] == "account_transactions"
        assert stage_args[0][2] == mock_conn
        
        # Check that merge was called with the right parameters
        mock_merge.assert_called_once()
        merge_args = mock_merge.call_args
        assert merge_args[0][0] == "general_ledger_staging.account_transactions"
        assert merge_args[0][1] == "general_ledger.account_transactions"
        assert merge_args[0][3] == mock_conn
        assert merge_args[0][4] == run_id
        # Check that the compound key is correctly used
        assert merge_args[1].get("id_matching_columns") == ["account_id", "transaction_id"]
