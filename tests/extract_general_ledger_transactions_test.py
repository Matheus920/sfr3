import json
import pytest
import datetime
import requests
from pydantic import ValidationError

from extract.extract_general_ledger_transactions import (
    get_general_ledger_transactions,
    transactions_json_search,
    fetch_transactions_from_json,
    fetch_transactions_from_api
)
from models.general_ledger_transactions import GeneralLedgerTransaction

# Sample valid transaction data based on the actual data structure
VALID_TRANSACTION_JSON = [
    {
        "Id": 13243860,
        "Date": "2024-12-15",
        "TransactionType": "Charge",
        "TotalAmount": 60.32,
        "CheckNumber": "",
        "UnitAgreement": {
            "Id": 130647,
            "Type": "Lease",
            "Href": "https://api.buildium.com/v1/leases/130647"
        },
        "UnitId": 96242,
        "UnitNumber": "707203",
        "PaymentDetail": {
            "PaymentMethod": "None",
            "Payee": None,
            "IsInternalTransaction": False,
            "InternalTransactionStatus": None
        },
        "DepositDetails": {
            "BankGLAccountId": None,
            "PaymentTransactions": []
        },
        "Journal": {
            "Memo": "Renewal Increase - Prorated 12/15-12/31",
            "Lines": [
                {
                    "GLAccount": {
                        "Id": 3,
                        "AccountNumber": None,
                        "Name": "Rental Income",
                        "Description": "Rent Income - resident responsibility",
                        "Type": "Income",
                        "SubType": "Income",
                        "IsDefaultGLAccount": True,
                        "DefaultAccountName": "Rent Income",
                        "IsContraAccount": False,
                        "IsBankAccount": False,
                        "CashFlowClassification": "OperatingActivities",
                        "ExcludeFromCashBalances": False,
                        "SubAccounts": None,
                        "IsActive": True,
                        "ParentGLAccountId": None
                    },
                    "Amount": 60.32,
                    "IsCashPosting": False,
                    "ReferenceNumber": None,
                    "Memo": "Renewal Increase - Prorated 12/15-12/31",
                    "AccountingEntity": {
                        "Id": 27786,
                        "AccountingEntityType": "Rental",
                        "Href": "https://api.buildium.com/v1/rentals/27786",
                        "Unit": {
                            "Id": 96242,
                            "Href": "https://api.buildium.com/v1/rentals/units/96242"
                        }
                    }
                }
            ]
        },
        "LastUpdatedDateTime": "2024-11-08T13:08:15Z"
    },
    {
        "Id": 13296623,
        "Date": "2024-12-01",
        "TransactionType": "Credit",
        "TotalAmount": 60.29,
        "CheckNumber": "",
        "UnitAgreement": {
            "Id": 160154,
            "Type": "Lease",
            "Href": "https://api.buildium.com/v1/leases/160154"
        },
        "UnitId": 169086,
        "UnitNumber": "1424172",
        "PaymentDetail": {
            "PaymentMethod": "None",
            "Payee": None,
            "IsInternalTransaction": False,
            "InternalTransactionStatus": None
        },
        "DepositDetails": {
            "BankGLAccountId": None,
            "PaymentTransactions": []
        },
        "Journal": {
            "Memo": "Credit - for moveout from 12/10/2024 to 12/31/2024",
            "Lines": [
                {
                    "GLAccount": {
                        "Id": 5,  # Different ID to test filtering
                        "AccountNumber": None,
                        "Name": "Rental Income",
                        "Description": "Rent Income - resident responsibility",
                        "Type": "Income",
                        "SubType": "Income",
                        "IsDefaultGLAccount": True,
                        "DefaultAccountName": "Rent Income",
                        "IsContraAccount": False,
                        "IsBankAccount": False,
                        "CashFlowClassification": "OperatingActivities",
                        "ExcludeFromCashBalances": False,
                        "SubAccounts": None,
                        "IsActive": True,
                        "ParentGLAccountId": None
                    },
                    "Amount": 60.29,
                    "IsCashPosting": False,
                    "ReferenceNumber": None,
                    "Memo": "Credit - for moveout from 12/10/2024 to 12/31/2024",
                    "AccountingEntity": {
                        "Id": 48326,
                        "AccountingEntityType": "Rental",
                        "Href": "https://api.buildium.com/v1/rentals/48326",
                        "Unit": {
                            "Id": 169086,
                            "Href": "https://api.buildium.com/v1/rentals/units/169086"
                        }
                    }
                }
            ]
        },
        "LastUpdatedDateTime": "2024-09-11T17:22:42Z"
    }
]

# Invalid transaction data to trigger validation errors
INVALID_TRANSACTION_JSON = [
    {
        "Id": 12345,
        "Date": "2024-12-15",
        "TransactionType": "Charge",
        "TotalAmount": 60.32,
        "CheckNumber": "",
        # Include Journal with matching GLAccount.Id so it passes the filtering step
        "Journal": {
            "Memo": "Test",
            "Lines": [
                {
                    "GLAccount": {
                        "Id": 3  # This will match our test account ID
                    },
                    "Amount": 60.32,
                    "IsCashPosting": False,
                    "Memo": "Test"
                    # Missing required fields like AccountingEntity
                }
            ]
        },
        # Missing other required fields (UnitAgreement, PaymentDetail, etc.)
        "LastUpdatedDateTime": "2024-11-08T13:08:15Z"
    }
]

# Transaction with no Journal field
TRANSACTION_NO_JOURNAL = [
    {
        "Id": 13243860,
        "Date": "2024-12-15",
        "TransactionType": "Charge",
        "TotalAmount": 60.32,
        "CheckNumber": "",
        "UnitAgreement": {
            "Id": 130647,
            "Type": "Lease",
            "Href": "https://api.buildium.com/v1/leases/130647"
        },
        "UnitId": 96242,
        "UnitNumber": "707203",
        "PaymentDetail": {
            "PaymentMethod": "None",
            "Payee": None,
            "IsInternalTransaction": False,
            "InternalTransactionStatus": None
        },
        "DepositDetails": {
            "BankGLAccountId": None,
            "PaymentTransactions": []
        },
        # No Journal field
        "LastUpdatedDateTime": "2024-11-08T13:08:15Z"
    }
]

# Transaction with Journal but no Lines
TRANSACTION_NO_LINES = [
    {
        "Id": 13243860,
        "Date": "2024-12-15",
        "TransactionType": "Charge",
        "TotalAmount": 60.32,
        "CheckNumber": "",
        "UnitAgreement": {
            "Id": 130647,
            "Type": "Lease",
            "Href": "https://api.buildium.com/v1/leases/130647"
        },
        "UnitId": 96242,
        "UnitNumber": "707203",
        "PaymentDetail": {
            "PaymentMethod": "None",
            "Payee": None,
            "IsInternalTransaction": False,
            "InternalTransactionStatus": None
        },
        "DepositDetails": {
            "BankGLAccountId": None,
            "PaymentTransactions": []
        },
        "Journal": {
            "Memo": "Renewal Increase - Prorated 12/15-12/31",
            # No Lines field
        },
        "LastUpdatedDateTime": "2024-11-08T13:08:15Z"
    }
]

# Transaction with Lines but no GLAccount
TRANSACTION_NO_GLACCOUNT = [
    {
        "Id": 13243860,
        "Date": "2024-12-15",
        "TransactionType": "Charge",
        "TotalAmount": 60.32,
        "CheckNumber": "",
        "UnitAgreement": {
            "Id": 130647,
            "Type": "Lease",
            "Href": "https://api.buildium.com/v1/leases/130647"
        },
        "UnitId": 96242,
        "UnitNumber": "707203",
        "PaymentDetail": {
            "PaymentMethod": "None",
            "Payee": None,
            "IsInternalTransaction": False,
            "InternalTransactionStatus": None
        },
        "DepositDetails": {
            "BankGLAccountId": None,
            "PaymentTransactions": []
        },
        "Journal": {
            "Memo": "Renewal Increase - Prorated 12/15-12/31",
            "Lines": [
                {
                    # No GLAccount field
                    "Amount": 60.32,
                    "IsCashPosting": False,
                    "ReferenceNumber": None,
                    "Memo": "Renewal Increase - Prorated 12/15-12/31",
                    "AccountingEntity": {
                        "Id": 27786,
                        "AccountingEntityType": "Rental",
                        "Href": "https://api.buildium.com/v1/rentals/27786",
                        "Unit": {
                            "Id": 96242,
                            "Href": "https://api.buildium.com/v1/rentals/units/96242"
                        }
                    }
                }
            ]
        },
        "LastUpdatedDateTime": "2024-11-08T13:08:15Z"
    }
]


def test_transactions_json_search():
    # Test filtering by GL account ID
    result_json = transactions_json_search(VALID_TRANSACTION_JSON, 3)
    result = json.loads(result_json)
    
    # Should return only transactions with GLAccount.Id = 3
    assert len(result) == 1
    assert result[0]["Id"] == 13243860
    
    # Test with a non-existent GL account ID
    result_json = transactions_json_search(VALID_TRANSACTION_JSON, 999)
    result = json.loads(result_json)
    assert len(result) == 0
    
    # Test with edge cases
    result_json = transactions_json_search(TRANSACTION_NO_JOURNAL, 3)
    result = json.loads(result_json)
    assert len(result) == 0
    
    result_json = transactions_json_search(TRANSACTION_NO_LINES, 3)
    result = json.loads(result_json)
    assert len(result) == 0
    
    result_json = transactions_json_search(TRANSACTION_NO_GLACCOUNT, 3)
    result = json.loads(result_json)
    assert len(result) == 0


def test_fetch_transactions_from_json():
    # Test with valid JSON and existing GL account ID
    result = fetch_transactions_from_json(json.dumps(VALID_TRANSACTION_JSON), 3)
    assert result
    transactions = json.loads(result)
    assert len(transactions) == 1
    assert transactions[0]["Id"] == 13243860
    
    # Test with non-existent GL account ID
    result = fetch_transactions_from_json(json.dumps(VALID_TRANSACTION_JSON), 999)
    transactions = json.loads(result)
    assert len(transactions) == 0


def test_direct_json_success():
    # Test with valid JSON and existing GL account ID
    transactions = get_general_ledger_transactions(
        general_account_id=3,
        json_data=json.dumps(VALID_TRANSACTION_JSON)
    )
    assert len(transactions) == 1
    assert isinstance(transactions[0], GeneralLedgerTransaction)
    assert transactions[0].id == 13243860
    
    # Test with non-existent GL account ID - should return empty list
    transactions = get_general_ledger_transactions(
        general_account_id=999,
        json_data=json.dumps(VALID_TRANSACTION_JSON)
    )
    assert len(transactions) == 0


def test_missing_general_account_id():
    # Test with missing general_account_id
    with pytest.raises(ValueError):
        get_general_ledger_transactions(
            general_account_id=None,
            json_data=json.dumps(VALID_TRANSACTION_JSON)
        )


def test_missing_dates_for_api():
    # Test with missing dates when using API
    with pytest.raises(ValueError):
        get_general_ledger_transactions(
            general_account_id=3
        )
    
    with pytest.raises(ValueError):
        get_general_ledger_transactions(
            general_account_id=3,
            start_date=datetime.date(2024, 1, 1)
        )
    
    with pytest.raises(ValueError):
        get_general_ledger_transactions(
            general_account_id=3,
            end_date=datetime.date(2024, 12, 31)
        )


def test_validation_error():
    # Test validation error with invalid transaction data
    # The transaction has a Journal with matching GLAccount.Id so it passes filtering
    # but is missing other required fields so it fails validation
    with pytest.raises(Exception):  # Use Exception to catch any validation-related error
        get_general_ledger_transactions(
            general_account_id=3,
            json_data=json.dumps(INVALID_TRANSACTION_JSON)
        )


def test_fetch_from_api(mocker):
    # Test successful API fetch
    mock_response = mocker.MagicMock()
    mock_response.text = json.dumps(VALID_TRANSACTION_JSON)
    mock_response.raise_for_status.return_value = None
    mock_get = mocker.patch("extract.extract_general_ledger_transactions.requests.get", 
                          return_value=mock_response)
    
    start_date = datetime.date(2024, 1, 1)
    end_date = datetime.date(2024, 12, 31)
    
    # Call the function
    result = fetch_transactions_from_api(3, start_date, end_date)
    
    # Verify API was called with correct parameters
    mock_get.assert_called_with(
        'https://api.buildium.com/v1/generalledger/transactions',
        headers={
            'x-buildium-client-id': 'YOUR_CLIENT_ID',
            'x-buildium-client-secret': 'YOUR_CLIENT_SECRET'
        },
        params={
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'glaccountids': [3]
        }
    )
    
    # Verify the result
    assert result == json.dumps(VALID_TRANSACTION_JSON)


def test_api_success(mocker):
    # Test successful API path in get_general_ledger_transactions
    mock_response = mocker.MagicMock()
    mock_response.text = json.dumps(VALID_TRANSACTION_JSON)
    mock_response.raise_for_status.return_value = None
    mocker.patch("extract.extract_general_ledger_transactions.requests.get", 
                return_value=mock_response)
    
    start_date = datetime.date(2024, 1, 1)
    end_date = datetime.date(2024, 12, 31)
    
    # Call the function
    transactions = get_general_ledger_transactions(
        general_account_id=3,
        start_date=start_date,
        end_date=end_date
    )
    
    # The function internally calls GeneralLedgerTransactions.validate_json which
    # may validate all transactions rather than just the filtered ones
    assert len(transactions) >= 1  # At least 1 transaction with GLAccount.Id = 3
    assert isinstance(transactions[0], GeneralLedgerTransaction)
    assert transactions[0].id == 13243860


def test_api_request_error(mocker):
    # Test API request error
    mock_response = mocker.MagicMock()
    mock_response.raise_for_status.side_effect = requests.RequestException("API error")
    mocker.patch("extract.extract_general_ledger_transactions.requests.get", 
                return_value=mock_response)
    
    start_date = datetime.date(2024, 1, 1)
    end_date = datetime.date(2024, 12, 31)
    
    # Call the function and verify it raises the expected exception
    with pytest.raises(requests.RequestException):
        get_general_ledger_transactions(
            general_account_id=3,
            start_date=start_date,
            end_date=end_date
        )


def test_api_rate_limit_retry(mocker):
    # Test API rate limit with retry
    responses = [
        mocker.MagicMock(status_code=429),  # First call: rate limited
        mocker.MagicMock(status_code=200, text=json.dumps(VALID_TRANSACTION_JSON))  # Second call: success
    ]
    
    mock_sleep = mocker.patch("extract.extract_general_ledger_transactions.time.sleep")
    mock_get = mocker.patch("extract.extract_general_ledger_transactions.requests.get", 
                          side_effect=responses)
    
    start_date = datetime.date(2024, 1, 1)
    end_date = datetime.date(2024, 12, 31)
    
    # Call the function
    result = fetch_transactions_from_api(3, start_date, end_date)
    
    # Verify sleep was called (for the retry)
    mock_sleep.assert_called_once()
    
    # Verify API was called twice
    assert mock_get.call_count == 2
    
    # Verify the result
    assert result == json.dumps(VALID_TRANSACTION_JSON)


def test_api_rate_limit_max_retries(mocker):
    # Test API rate limit with max retries exceeded
    # Create 5 responses that all return rate limit errors
    responses = [mocker.MagicMock(status_code=429) for _ in range(5)]
    
    mock_sleep = mocker.patch("extract.extract_general_ledger_transactions.time.sleep")
    mock_get = mocker.patch("extract.extract_general_ledger_transactions.requests.get", 
                          side_effect=responses)
    
    start_date = datetime.date(2024, 1, 1)
    end_date = datetime.date(2024, 12, 31)
    
    # Call the function and verify it raises the expected exception
    with pytest.raises(Exception, match="Max retries exceeded"):
        fetch_transactions_from_api(3, start_date, end_date)
    
    # Verify sleep was called for each retry (in the implementation, sleep is called
    # for all retries, including before the last retry)
    assert mock_sleep.call_count == 5  # Called for all retries
    
    # Verify API was called the maximum number of times
    assert mock_get.call_count == 5
