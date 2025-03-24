import pytest
import json
from datetime import date, datetime

from models.general_ledger_transactions import (
    GeneralLedgerTransaction,
    UnitAgreement,
    PaymentDetail,
    DepositDetails,
    Journal,
    JournalLine,
    AccountingEntity,
    Unit
)
from models.general_ledger_account import GeneralLedgerAccount
from models.general_ledger_account_transactions import GeneralLedgerAccountTransactions
from transform.transform_account_participation import map_account_participation


# Reuse the helper function from transform_general_ledger_transactions_test.py
def create_test_transaction(id=1, account_id=10, amount=100.00, memo="Test Transaction"):
    # Create a GL account for the journal line
    gl_account = GeneralLedgerAccount(
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
    
    # Create a journal line
    journal_line = JournalLine(
        GLAccount=gl_account,
        Amount=amount,
        IsCashPosting=False,
        ReferenceNumber="REF123",
        Memo=memo,
        AccountingEntity=accounting_entity
    )
    
    # Create a journal
    journal = Journal(
        Memo=memo,
        Lines=[journal_line]
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
    transaction = GeneralLedgerTransaction(
        Id=id,
        Date=date(2024, 3, 15),
        TransactionType="Charge",
        TotalAmount=amount,
        CheckNumber="12345",
        UnitAgreement=unit_agreement,
        UnitId=400,
        UnitNumber="Unit400",
        PaymentDetail=payment_detail,
        DepositDetails=deposit_details,
        Journal=journal,
        LastUpdatedDateTime=datetime(2024, 3, 15, 12, 0, 0)
    )
    
    return transaction


# Mock for get_general_ledger_transactions to return predefined transactions
def mock_get_transactions(monkeypatch, account_to_transaction_map: dict):
    """
    Configure a mock for get_general_ledger_transactions that returns specific transactions
    based on the account ID provided.
    """
    def mock_implementation(json_data, general_account_id):
        # Return predefined transactions for this account ID, or empty list if none
        return account_to_transaction_map.get(general_account_id, [])
    
    # Apply the mock
    monkeypatch.setattr(
        "transform.transform_account_participation.get_general_ledger_transactions", 
        mock_implementation
    )


def test_map_account_participation_with_transactions(monkeypatch):
    """Test mapping accounts to transactions when transactions exist."""
    # Set up test data
    account_ids = {101, 102}
    json_data = '{"dummy": "data"}'
    
    # Create test transactions
    transaction1 = create_test_transaction(id=1001, account_id=101)
    transaction2 = create_test_transaction(id=1002, account_id=102)
    
    # Configure the mock to return specific transactions for each account
    account_to_transactions = {
        101: [transaction1],
        102: [transaction2]
    }
    mock_get_transactions(monkeypatch, account_to_transactions)
    
    # Call the function under test
    transactions_per_account, all_transactions = map_account_participation(
        account_ids, json_data
    )
    
    # Verify results
    assert len(transactions_per_account) == 2
    assert len(all_transactions) == 2
    
    # Check account-transaction mappings (order may vary)
    mappings = {(tx.account_id, tx.transaction_id) for tx in transactions_per_account}
    assert (101, 1001) in mappings
    assert (102, 1002) in mappings


def test_map_account_participation_no_transactions(monkeypatch):
    """Test mapping accounts to transactions when no transactions exist."""
    # Set up test data with no transactions
    account_ids = {101}
    json_data = '{}'
    
    # Configure the mock to return no transactions
    mock_get_transactions(monkeypatch, {})
    
    # Call the function under test
    transactions_per_account, all_transactions = map_account_participation(
        account_ids, json_data
    )
    
    # Verify results
    assert len(transactions_per_account) == 0
    assert len(all_transactions) == 0


def test_map_account_participation_mixed_results(monkeypatch):
    """Test mapping accounts to transactions with mixed results (some accounts have transactions, others don't)."""
    # Set up test data
    account_ids = {101, 102, 103}
    json_data = '{"dummy": "data"}'
    
    # Create test transactions
    transaction1 = create_test_transaction(id=1001, account_id=101)
    transaction2 = create_test_transaction(id=1002, account_id=102)
    
    # Configure the mock to return transactions for some accounts but not others
    account_to_transactions = {
        101: [transaction1],
        102: [transaction2],
        103: []  # No transactions for account 103
    }
    mock_get_transactions(monkeypatch, account_to_transactions)
    
    # Call the function under test
    transactions_per_account, all_transactions = map_account_participation(
        account_ids, json_data
    )
    
    # Verify results
    assert len(transactions_per_account) == 2  # Only two accounts have transactions
    assert len(all_transactions) == 2
    
    # Check account-transaction mappings
    mappings = {(tx.account_id, tx.transaction_id) for tx in transactions_per_account}
    assert (101, 1001) in mappings
    assert (102, 1002) in mappings
    assert not any(tx.account_id == 103 for tx in transactions_per_account)


def test_map_account_participation_multiple_transactions_per_account(monkeypatch):
    """Test mapping accounts to transactions when an account has multiple transactions."""
    # Set up test data
    account_ids = {101}
    json_data = '{"dummy": "data"}'
    
    # Create multiple test transactions for the same account
    transaction1 = create_test_transaction(id=1001, account_id=101)
    transaction2 = create_test_transaction(id=1002, account_id=101)
    
    # Configure the mock to return multiple transactions for an account
    account_to_transactions = {
        101: [transaction1, transaction2]
    }
    mock_get_transactions(monkeypatch, account_to_transactions)
    
    # Call the function under test
    transactions_per_account, all_transactions = map_account_participation(
        account_ids, json_data
    )
    
    # Verify results
    assert len(transactions_per_account) == 2  # Two mappings for one account
    assert len(all_transactions) == 2
    
    # Check account-transaction mappings
    mappings = {(tx.account_id, tx.transaction_id) for tx in transactions_per_account}
    assert (101, 1001) in mappings
    assert (101, 1002) in mappings
