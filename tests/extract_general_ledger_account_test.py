import json
import pytest
import requests
from pydantic import ValidationError

from extract.extract_general_ledger_accounts import get_general_ledger_accounts
from models.general_ledger_account import GeneralLedgerAccount

# Dummy valid account data for testing with all required fields.
VALID_ACCOUNT_JSON = [
    {
        "Id": 1,
        "AccountNumber": "ACCT100",
        "Name": "Cash Account",
        "Description": "Primary cash account",
        "Type": "Asset",
        "SubType": "Cash",
        "IsDefaultGLAccount": True,
        "DefaultAccountName": "Cash Account Default",
        "IsContraAccount": False,
        "IsBankAccount": True,
        "CashFlowClassification": "Operating",
        "ExcludeFromCashBalances": False,
        "SubAccounts": [],
        "IsActive": True,
        "ParentGLAccountId": None
    },
    {
        "Id": 2,
        "AccountNumber": "ACCT200",
        "Name": "Receivables",
        "Description": "Accounts Receivable account",
        "Type": "Asset",
        "SubType": "Receivable",
        "IsDefaultGLAccount": False,
        "DefaultAccountName": None,
        "IsContraAccount": False,
        "IsBankAccount": False,
        "CashFlowClassification": "Operating",
        "ExcludeFromCashBalances": False,
        "SubAccounts": [],
        "IsActive": True,
        "ParentGLAccountId": None
    }
]

# Dummy invalid account data to trigger validation error.
INVALID_ACCOUNT_JSON = [
    {
        # Missing required fields: "Id", "Name", "Type", "SubType", "IsDefaultGLAccount", etc.
    }
]

def test_direct_json_success():
    # Direct JSON data approach - no mocking needed
    accounts = get_general_ledger_accounts(json_data=json.dumps(VALID_ACCOUNT_JSON))
    # Check that we successfully read all accounts.
    assert len(accounts) == len(VALID_ACCOUNT_JSON)
    # Check that each account is an instance of GeneralLedgerAccount.
    for account in accounts:
        assert isinstance(account, GeneralLedgerAccount)

def test_validation_error_on_account():
    # Direct JSON data approach for invalid data
    with pytest.raises(ValidationError):
        get_general_ledger_accounts(json_data=json.dumps(INVALID_ACCOUNT_JSON))

def test_api_success(mocker):
    mock_response = mocker.MagicMock()
    mock_response.text = json.dumps(VALID_ACCOUNT_JSON)
    mock_response.raise_for_status.return_value = None
    mock_get = mocker.patch("extract.extract_general_ledger_accounts.requests.get", return_value=mock_response)

    # No json_data provided, so it should use the API
    accounts = get_general_ledger_accounts()
    assert len(accounts) == len(VALID_ACCOUNT_JSON)
    for account in accounts:
        assert isinstance(account, GeneralLedgerAccount)
    mock_get.assert_called_with(
        'https://api.buildium.com/v1/glaccounts',
        headers={
            'x-buildium-client-id': 'YOUR_CLIENT_ID',
            'x-buildium-client-secret': 'YOUR_CLIENT_SECRET'
        }
    )

def test_api_request_error(mocker):
    mock_response = mocker.MagicMock()
    mock_response.raise_for_status.side_effect = requests.RequestException("API error")
    mocker.patch("extract.extract_general_ledger_accounts.requests.get", return_value=mock_response)

    # No json_data provided, so it should use the API which will raise an error
    with pytest.raises(requests.RequestException):
        get_general_ledger_accounts()
