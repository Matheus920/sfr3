from __future__ import annotations
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from typing import Optional

class GeneralLedgerAccount(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: int = Field(..., alias="Id")
    account_number: Optional[str] = Field(None, alias="AccountNumber")
    name: str = Field(..., alias="Name")
    description: Optional[str] = Field(None, alias="Description")
    type: str = Field(..., alias="Type")
    sub_type: str = Field(..., alias="SubType")
    is_default_gl_account: bool = Field(..., alias="IsDefaultGLAccount")
    default_account_name: Optional[str] = Field(None, alias="DefaultAccountName")
    is_contra_account: bool = Field(..., alias="IsContraAccount")
    is_bank_account: bool = Field(..., alias="IsBankAccount")
    cash_flow_classification: Optional[str] = Field(None, alias="CashFlowClassification")
    exclude_from_cash_balances: bool = Field(..., alias="ExcludeFromCashBalances")
    sub_accounts: Optional[list[GeneralLedgerAccount]] = Field(default_factory=list, alias="SubAccounts")
    is_active: bool = Field(..., alias="IsActive")
    parent_gl_account_id: Optional[int] = Field(None, alias="ParentGLAccountId")

class GeneralLedgerAccountFlattened(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: int = Field(..., alias="Id")
    account_number: Optional[str] = Field(None, alias="AccountNumber")
    name: str = Field(..., alias="Name")
    description: Optional[str] = Field(None, alias="Description")
    type: str = Field(..., alias="Type")
    sub_type: str = Field(..., alias="SubType")
    is_default_gl_account: bool = Field(..., alias="IsDefaultGLAccount")
    default_account_name: Optional[str] = Field(None, alias="DefaultAccountName")
    is_contra_account: bool = Field(..., alias="IsContraAccount")
    is_bank_account: bool = Field(..., alias="IsBankAccount")
    cash_flow_classification: Optional[str] = Field(None, alias="CashFlowClassification")
    exclude_from_cash_balances: bool = Field(..., alias="ExcludeFromCashBalances")
    is_active: bool = Field(..., alias="IsActive")
    parent_gl_account_id: Optional[int] = Field(None, alias="ParentGLAccountId")

GeneralLedgerAccounts = TypeAdapter(list[GeneralLedgerAccount])
GeneralLedgerAccountsFlattened = TypeAdapter(list[GeneralLedgerAccountFlattened])