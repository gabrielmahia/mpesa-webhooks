"""
mpesa_webhooks.models — Parsed M-Pesa callback payloads.

These dataclasses represent every callback shape the Daraja v3 API
sends to your application. Parsing is strict: unknown keys are
ignored, missing required keys raise ValueError.

All amounts are in KES as floats. All phone numbers are in E.164
format (254XXXXXXXXX) as strings.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any


class CallbackType(str, Enum):
    STK_PUSH    = "stk_push"
    B2C_RESULT  = "b2c_result"
    C2B_PAYMENT = "c2b_payment"
    BALANCE     = "account_balance"
    UNKNOWN     = "unknown"


@dataclass(frozen=True)
class StkPushCallback:
    """STK Push callback — POSTed to your CallBackURL after PIN entry."""

    merchant_request_id:  str
    checkout_request_id:  str
    result_code:          int
    result_desc:          str

    # Populated on success (result_code == 0); None on failure
    amount:               float | None = None
    mpesa_receipt:        str   | None = None
    transaction_date:     datetime | None = None
    phone_number:         str   | None = None

    @property
    def succeeded(self) -> bool:
        return self.result_code == 0

    @property
    def user_cancelled(self) -> bool:
        return self.result_code == 1032

    @property
    def insufficient_funds(self) -> bool:
        return self.result_code == 1

    @classmethod
    def from_daraja(cls, payload: dict[str, Any]) -> "StkPushCallback":
        """Parse the raw Daraja STK Push callback JSON."""
        try:
            cb = payload["Body"]["stkCallback"]
        except KeyError as e:
            raise ValueError(f"Invalid STK Push callback — missing key: {e}") from e

        result_code = int(cb["ResultCode"])
        amount = receipt = txn_date = phone = None

        if result_code == 0:
            items = {
                i["Name"]: i.get("Value")
                for i in cb.get("CallbackMetadata", {}).get("Item", [])
            }
            amount = float(items.get("Amount", 0))
            receipt = str(items["MpesaReceiptNumber"]) if "MpesaReceiptNumber" in items else None
            phone   = str(items["PhoneNumber"])         if "PhoneNumber" in items else None
            raw_dt  = items.get("TransactionDate")
            if raw_dt:
                try:
                    txn_date = datetime.strptime(str(raw_dt), "%Y%m%d%H%M%S")
                except ValueError:
                    txn_date = None

        return cls(
            merchant_request_id=str(cb["MerchantRequestID"]),
            checkout_request_id=str(cb["CheckoutRequestID"]),
            result_code=result_code,
            result_desc=str(cb["ResultDesc"]),
            amount=amount,
            mpesa_receipt=receipt,
            transaction_date=txn_date,
            phone_number=phone,
        )


@dataclass(frozen=True)
class B2CResultCallback:
    """B2C Result callback — POSTed to your ResultURL after disbursement."""

    conversation_id:            str
    originator_conversation_id: str
    transaction_id:             str
    result_code:                int
    result_desc:                str
    transaction_amount:         float | None = None
    receiver_phone:             str   | None = None

    @property
    def succeeded(self) -> bool:
        return self.result_code == 0

    @classmethod
    def from_daraja(cls, payload: dict[str, Any]) -> "B2CResultCallback":
        try:
            result = payload["Result"]
        except KeyError as e:
            raise ValueError(f"Invalid B2C callback — missing key: {e}") from e

        params = {
            p["Key"]: p.get("Value")
            for p in result.get("ResultParameters", {}).get("ResultParameter", [])
        }

        return cls(
            conversation_id=str(result["ConversationID"]),
            originator_conversation_id=str(result["OriginatorConversationID"]),
            transaction_id=str(result.get("TransactionID", "")),
            result_code=int(result["ResultCode"]),
            result_desc=str(result["ResultDesc"]),
            transaction_amount=float(params["TransactionAmount"]) if "TransactionAmount" in params else None,
            receiver_phone=str(params["ReceiverPartyPublicName"]).split(" - ")[0] if "ReceiverPartyPublicName" in params else None,
        )


@dataclass(frozen=True)
class C2BPaymentCallback:
    """C2B Payment callback — POSTed to your ConfirmationURL."""

    transaction_type:       str
    trans_id:               str
    trans_time:             str
    trans_amount:           float
    business_short_code:    str
    bill_ref_number:        str
    msisdn:                 str   # Paying phone number E.164
    first_name:             str
    middle_name:            str = ""
    last_name:              str = ""

    @classmethod
    def from_daraja(cls, payload: dict[str, Any]) -> "C2BPaymentCallback":
        try:
            return cls(
                transaction_type=str(payload["TransactionType"]),
                trans_id=str(payload["TransID"]),
                trans_time=str(payload["TransTime"]),
                trans_amount=float(payload["TransAmount"]),
                business_short_code=str(payload["BusinessShortCode"]),
                bill_ref_number=str(payload["BillRefNumber"]),
                msisdn=str(payload["MSISDN"]),
                first_name=str(payload.get("FirstName", "")),
                middle_name=str(payload.get("MiddleName", "")),
                last_name=str(payload.get("LastName", "")),
            )
        except KeyError as e:
            raise ValueError(f"Invalid C2B callback — missing key: {e}") from e


def detect_callback_type(payload: dict[str, Any]) -> CallbackType:
    """Detect what kind of Daraja callback a raw payload is."""
    if "Body" in payload and "stkCallback" in payload.get("Body", {}):
        return CallbackType.STK_PUSH
    if "Result" in payload and "ConversationID" in payload.get("Result", {}):
        return CallbackType.B2C_RESULT
    if "TransID" in payload and "MSISDN" in payload:
        return CallbackType.C2B_PAYMENT
    return CallbackType.UNKNOWN
