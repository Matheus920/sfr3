from pydantic import BaseModel, ConfigDict, Field


class GeneralLedgerAccountTransactions(BaseModel):
    account_id: int = Field(...)
    transaction_id: int = Field(...)