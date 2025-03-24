from transform.transform_general_ledger_accounts import transform_general_ledger_accounts
from models.general_ledger_account import GeneralLedgerAccount

def test_single_account_no_sub_accounts():
    # Test transforming one account with no sub-accounts.
    account = GeneralLedgerAccount(
        Id=1,
        Name="Main Account",
        Type="Asset",
        SubType="Current Asset",
        IsDefaultGLAccount=False,
        IsContraAccount=False,
        IsBankAccount=False,
        ExcludeFromCashBalances=False,
        IsActive=True
    )
    transformed = transform_general_ledger_accounts([account])
    # Only one flattened account is expected.
    assert len(transformed) == 1
    flattened = transformed[0]
    assert flattened.id == 1
    assert flattened.name == "Main Account"

def test_account_with_sub_accounts():
    # Test transforming an account with multiple sub-accounts.
    sub1 = GeneralLedgerAccount(
        Id=2,
        Name="Sub Account 1",
        Type="Asset",
        SubType="Current Asset",
        IsDefaultGLAccount=False,
        IsContraAccount=False,
        IsBankAccount=False,
        ExcludeFromCashBalances=False,
        IsActive=True,
        ParentGLAccountId=1
    )
    sub2 = GeneralLedgerAccount(
        Id=3,
        Name="Sub Account 2",
        Type="Asset",
        SubType="Current Asset",
        IsDefaultGLAccount=False,
        IsContraAccount=False,
        IsBankAccount=False,
        ExcludeFromCashBalances=False,
        IsActive=True,
        ParentGLAccountId=1
    )
    account = GeneralLedgerAccount(
        Id=1,
        Name="Main Account",
        Type="Asset",
        SubType="Current Asset",
        IsDefaultGLAccount=False,
        IsContraAccount=False,
        IsBankAccount=False,
        ExcludeFromCashBalances=False,
        IsActive=True,
        SubAccounts=[sub1, sub2]
    )
    transformed = transform_general_ledger_accounts([account])
    # Expect 1 flattened parent + 2 flattened sub-accounts = 3 entries.
    assert len(transformed) == 3
    # Check parent's fields.
    assert transformed[0].id == 1
    assert transformed[0].name == "Main Account"
    # Check sub-account 1.
    assert transformed[1].id == 2
    assert transformed[1].name == "Sub Account 1"
    # Check sub-account 2.
    assert transformed[2].id == 3
    assert transformed[2].name == "Sub Account 2"
