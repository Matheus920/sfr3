from __future__ import annotations
from datetime import date as date_type, datetime
from typing import List, Optional, Any
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from models.general_ledger_account import GeneralLedgerAccount


class UnitAgreement(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: int = Field(..., alias="Id")
    type: str = Field(..., alias="Type")
    href: str = Field(..., alias="Href")


class PaymentDetail(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    payment_method: str = Field(..., alias="PaymentMethod")
    payee: Optional[str] = Field(None, alias="Payee")
    is_internal_transaction: bool = Field(..., alias="IsInternalTransaction")
    internal_transaction_status: Optional[str] = Field(
        None, alias="InternalTransactionStatus"
    )


class DepositDetails(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    bank_gl_account_id: Optional[int] = Field(None, alias="BankGLAccountId")
    payment_transactions: List[Any] = Field(
        default_factory=list, alias="PaymentTransactions"
    )


class Unit(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: int = Field(..., alias="Id")
    href: str = Field(..., alias="Href")


class AccountingEntity(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: int = Field(..., alias="Id")
    accounting_entity_type: str = Field(..., alias="AccountingEntityType")
    href: str = Field(..., alias="Href")
    unit: Unit = Field(..., alias="Unit")


class JournalLine(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    gl_account: GeneralLedgerAccount = Field(..., alias="GLAccount")
    amount: float = Field(..., alias="Amount")
    is_cash_posting: bool = Field(..., alias="IsCashPosting")
    reference_number: Optional[str] = Field(None, alias="ReferenceNumber")
    memo: str = Field(..., alias="Memo")
    accounting_entity: AccountingEntity = Field(..., alias="AccountingEntity")


class JournalLineTransformed(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    amount: float = Field(..., alias="Amount")
    is_cash_posting: bool = Field(..., alias="IsCashPosting")
    reference_number: Optional[str] = Field(None, alias="ReferenceNumber")
    memo: str = Field(..., alias="Memo")
    general_ledger_account_id: int = Field(...)
    accounting_entity: AccountingEntity = Field(..., alias="AccountingEntity")


class Journal(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    memo: str = Field(..., alias="Memo")
    lines: List[JournalLine] = Field(..., alias="Lines")


class GeneralLedgerTransaction(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: int = Field(..., alias="Id")
    date: date_type = Field(..., alias="Date")
    transaction_type: str = Field(..., alias="TransactionType")
    total_amount: float = Field(..., alias="TotalAmount")
    check_number: str = Field(..., alias="CheckNumber")
    unit_agreement: UnitAgreement = Field(..., alias="UnitAgreement")
    unit_id: int = Field(..., alias="UnitId")
    unit_number: str = Field(..., alias="UnitNumber")
    payment_detail: PaymentDetail = Field(..., alias="PaymentDetail")
    deposit_details: DepositDetails = Field(..., alias="DepositDetails")
    journal: Journal = Field(..., alias="Journal")
    last_updated_date_time: datetime = Field(..., alias="LastUpdatedDateTime")


class GeneralLedgerTransactionTransformed(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: int = Field(..., alias="Id")
    date: date_type = Field(..., alias="Date")
    transaction_type: str = Field(..., alias="TransactionType")
    total_amount: float = Field(..., alias="TotalAmount")
    check_number: str = Field(..., alias="CheckNumber")
    unit_agreement: UnitAgreement = Field(..., alias="UnitAgreement")
    unit_id: int = Field(..., alias="UnitId")
    unit_number: str = Field(..., alias="UnitNumber")
    payment_detail: PaymentDetail = Field(..., alias="PaymentDetail")
    deposit_details: DepositDetails = Field(..., alias="DepositDetails")
    journal_memo: str = Field(...)
    lines: list[JournalLineTransformed] = Field(...)
    last_updated_date_time: datetime = Field(..., alias="LastUpdatedDateTime")

GeneralLedgerTransactions = TypeAdapter(List[GeneralLedgerTransaction])
