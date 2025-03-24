import pytest
from datetime import date, datetime

from models.general_ledger_transactions import (
    GeneralLedgerTransaction,
    GeneralLedgerTransactionTransformed,
    Journal,
    JournalLine,
    UnitAgreement,
    PaymentDetail,
    DepositDetails,
    AccountingEntity,
    Unit
)
from models.general_ledger_account import GeneralLedgerAccount
from transform.transform_general_ledger_transactions import transform_general_ledger_transactions


# Helper function to create a basic transaction for testing
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


def test_empty_input():
    """Test that an empty list returns an empty list."""
    result = transform_general_ledger_transactions([])
    
    # Verify the result is an empty list
    assert isinstance(result, list)
    assert len(result) == 0


def test_single_transaction():
    """Test transforming a single transaction."""
    # Create a test transaction
    transaction = create_test_transaction(id=1, account_id=10, amount=100.00)
    
    # Transform the transactions
    result = transform_general_ledger_transactions([transaction])
    
    # Verify the result
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], GeneralLedgerTransactionTransformed)
    
    # Check basic fields were correctly transferred
    transformed = result[0]
    assert transformed.id == 1
    assert transformed.date == date(2024, 3, 15)
    assert transformed.transaction_type == "Charge"
    assert transformed.total_amount == 100.00
    assert transformed.check_number == "12345"
    
    # Check the journal data was correctly transformed
    assert transformed.journal_memo == "Test Transaction"
    assert len(transformed.lines) == 1
    assert transformed.lines[0].amount == 100.00
    assert transformed.lines[0].memo == "Test Transaction"


def test_multiple_transactions():
    """Test transforming multiple transactions."""
    # Create test transactions
    transaction1 = create_test_transaction(id=1, account_id=10, amount=100.00, memo="Transaction 1")
    transaction2 = create_test_transaction(id=2, account_id=10, amount=200.00, memo="Transaction 2")
    
    # Transform the transactions
    result = transform_general_ledger_transactions([transaction1, transaction2])
    
    # Verify the result
    assert isinstance(result, list)
    assert len(result) == 2
    
    # Sort results by ID to ensure consistent order for testing
    result.sort(key=lambda x: x.id)
    
    # Verify first transaction
    assert result[0].id == 1
    assert result[0].total_amount == 100.00
    assert result[0].journal_memo == "Transaction 1"
    
    # Verify second transaction
    assert result[1].id == 2
    assert result[1].total_amount == 200.00
    assert result[1].journal_memo == "Transaction 2"


def test_transactions_with_different_accounts():
    """Test transforming transactions with different GL accounts."""
    # Create test transactions for different accounts
    transaction1 = create_test_transaction(id=1, account_id=10, amount=100.00, memo="Account 10 Transaction")
    transaction2 = create_test_transaction(id=2, account_id=20, amount=200.00, memo="Account 20 Transaction")
    
    # Transform the transactions
    result = transform_general_ledger_transactions([transaction1, transaction2])
    
    # Verify the result
    assert isinstance(result, list)
    assert len(result) == 2
    
    # Sort results by ID to ensure consistent order for testing
    result.sort(key=lambda x: x.id)
    
    # Verify first transaction
    assert result[0].id == 1
    assert result[0].total_amount == 100.00
    assert result[0].journal_memo == "Account 10 Transaction"
    
    # Verify second transaction
    assert result[1].id == 2
    assert result[1].total_amount == 200.00
    assert result[1].journal_memo == "Account 20 Transaction"


def test_multiple_journal_lines():
    """Test transforming a transaction with multiple journal lines."""
    # Create a transaction with multiple journal lines
    transaction = create_test_transaction(id=1, account_id=10, amount=100.00)
    
    # Add another journal line to the transaction
    gl_account = GeneralLedgerAccount(
        Id=10,
        Name="Secondary Account",
        Type="Expense",
        SubType="OperatingExpense",
        IsDefaultGLAccount=False,
        IsContraAccount=False,
        IsBankAccount=False,
        CashFlowClassification="OperatingActivities",
        ExcludeFromCashBalances=False,
        IsActive=True
    )
    
    unit = Unit(
        Id=500,
        Href="https://api.example.com/units/500"
    )
    
    accounting_entity = AccountingEntity(
        Id=200,
        AccountingEntityType="Rental",
        Href="https://api.example.com/rentals/200",
        Unit=unit
    )
    
    second_line = JournalLine(
        GLAccount=gl_account,
        Amount=50.00,
        IsCashPosting=True,
        ReferenceNumber="REF456",
        Memo="Secondary Line",
        AccountingEntity=accounting_entity
    )
    
    # Add the second line to the transaction's journal
    transaction.journal.lines.append(second_line)
    
    # Transform the transactions
    result = transform_general_ledger_transactions([transaction])
    
    # Verify the result
    assert isinstance(result, list)
    assert len(result) == 1
    
    # Verify transformed transaction
    transformed = result[0]
    assert transformed.id == 1
    
    # Verify journal lines were correctly transformed
    assert len(transformed.lines) == 2
    assert transformed.lines[0].amount == 100.00
    assert transformed.lines[0].memo == "Test Transaction"
    assert transformed.lines[1].amount == 50.00
    assert transformed.lines[1].memo == "Secondary Line"
    assert transformed.lines[1].is_cash_posting == True
    
    # Verify gl_account field was excluded
    with pytest.raises(AttributeError):
        assert transformed.lines[0].gl_account


def test_field_exclusion_inclusion():
    """Test that fields are correctly excluded and included in the transformation."""
    # Create a test transaction
    transaction = create_test_transaction(id=1, account_id=10)
    
    # Transform the transactions
    result = transform_general_ledger_transactions([transaction])
    
    # Verify the result
    assert isinstance(result, list)
    assert len(result) == 1
    
    transformed = result[0]
    
    # Check that journal field is excluded
    with pytest.raises(AttributeError):
        assert transformed.journal
    
    # Check that journal_memo is included
    assert transformed.journal_memo == "Test Transaction"
    
    # Check that gl_account is excluded from lines
    with pytest.raises(AttributeError):
        assert transformed.lines[0].gl_account


def test_empty_journal_lines():
    """Test transforming a transaction with empty journal lines."""
    # Create a transaction
    transaction = create_test_transaction(id=1, account_id=10)
    
    # Replace journal lines with an empty list
    transaction.journal.lines = []
    
    # Transform the transactions
    result = transform_general_ledger_transactions([transaction])
    
    # Verify the result
    assert isinstance(result, list)
    assert len(result) == 1
    
    # Verify transformed transaction has empty lines
    transformed = result[0]
    assert transformed.id == 1
    assert transformed.journal_memo == "Test Transaction"
    assert transformed.lines == []


def test_preserve_all_fields():
    """Test that all fields from the original transaction are preserved."""
    # Create a test transaction
    transaction = create_test_transaction(id=1, account_id=10)
    
    # Transform the transactions
    result = transform_general_ledger_transactions([transaction])
    
    # Verify the result
    assert isinstance(result, list)
    assert len(result) == 1
    
    # Get the original transaction data (excluding journal)
    original_data = transaction.model_dump(exclude={"journal"})
    
    # Verify that all original fields are in the transformed transaction
    transformed = result[0]
    for key in original_data:
        assert hasattr(transformed, key), f"Field {key} is missing from transformed transaction"
        if key == "unit_agreement":
            # For nested objects, check that their ID matches
            assert transformed.unit_agreement.id == original_data[key]["id"]
        elif isinstance(original_data[key], dict) or isinstance(original_data[key], list):
            # Skip complex comparisons for other nested objects
            pass
        else:
            # For simple fields, check that the values match
            assert getattr(transformed, key) == original_data[key], f"Field {key} has incorrect value"


def test_duplicate_transaction_handling():
    """Test that duplicate transactions (with same ID) are handled correctly."""
    # Create two transactions with the same ID but different details
    transaction1 = create_test_transaction(id=1, account_id=10, amount=100.00, memo="Original Transaction")
    transaction2 = create_test_transaction(id=1, account_id=20, amount=200.00, memo="Duplicate Transaction")
    transaction3 = create_test_transaction(id=2, account_id=30, amount=300.00, memo="Different Transaction")
    
    # Transform the transactions with duplicates
    result = transform_general_ledger_transactions([transaction1, transaction2, transaction3])
    
    # Verify the result
    assert isinstance(result, list)
    # Should only have 2 transactions, as one is a duplicate
    assert len(result) == 2
    
    # Sort results by ID to ensure consistent order for testing
    result.sort(key=lambda x: x.id)
    
    # Verify first transaction (should be the original, not the duplicate)
    assert result[0].id == 1
    assert result[0].total_amount == 100.00
    assert result[0].journal_memo == "Original Transaction"
    
    # Verify second transaction
    assert result[1].id == 2
    assert result[1].total_amount == 300.00
    assert result[1].journal_memo == "Different Transaction"
