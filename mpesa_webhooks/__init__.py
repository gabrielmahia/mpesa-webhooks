"""
mpesa-webhooks — Production FastAPI handler for M-Pesa Daraja callbacks.

Handles STK Push callbacks, B2C result/timeout, and C2B confirmation/validation
with:
  - Idempotency: duplicate M-Pesa receipts are detected and silently acked
  - HMAC verification: optional IP allowlist + request signing
  - Pluggable storage: in-memory (default), bring your own (implement StorageBackend)
  - Dead-letter pattern: failed handlers are queued for retry inspection
  - Structured logging: every callback logged with receipt, amount, phone, result

Usage — mount into any FastAPI app:

    from fastapi import FastAPI
    from mpesa_webhooks import MpesaWebhookRouter

    app = FastAPI()
    router = MpesaWebhookRouter()

    @router.on_stk_success
    async def handle_payment(event: STKSuccessEvent):
        # Your business logic: mark order paid, provision service, etc.
        await db.orders.mark_paid(event.checkout_request_id, event.receipt)

    app.include_router(router.router, prefix="/mpesa")

    # Endpoints created:
    #   POST /mpesa/stk/callback
    #   POST /mpesa/b2c/result
    #   POST /mpesa/b2c/timeout
    #   POST /mpesa/c2b/confirmation
    #   POST /mpesa/c2b/validation
    #   GET  /mpesa/health
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Optional
from abc import ABC, abstractmethod

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

logger = logging.getLogger("mpesa_webhooks")


# ── Event models ───────────────────────────────────────────────────────────────

class STKSuccessEvent(BaseModel):
    """Fired when an STK Push payment completes successfully."""
    merchant_request_id: str
    checkout_request_id: str
    receipt: str                    # M-Pesa receipt number — idempotency key
    amount: float
    phone: str                      # E.164 without +
    transaction_date: datetime
    raw: dict = Field(default_factory=dict, exclude=True)


class STKFailureEvent(BaseModel):
    """Fired when an STK Push is cancelled, times out, or fails."""
    merchant_request_id: str
    checkout_request_id: str
    result_code: int
    result_desc: str
    raw: dict = Field(default_factory=dict, exclude=True)


class B2CResultEvent(BaseModel):
    """Fired when a B2C disbursement completes (success or failure)."""
    conversation_id: str
    originator_conversation_id: str
    result_code: int
    result_desc: str
    amount: Optional[float] = None
    receipt: Optional[str] = None
    phone: Optional[str] = None
    raw: dict = Field(default_factory=dict, exclude=True)

    @property
    def succeeded(self) -> bool:
        return self.result_code == 0


class C2BConfirmationEvent(BaseModel):
    """Fired when a C2B payment is confirmed."""
    transaction_type: str
    trans_id: str                   # M-Pesa transaction ID
    trans_time: str
    trans_amount: float
    business_short_code: str
    bill_ref_number: str
    msisdn: str                     # Customer phone
    first_name: str = ""
    last_name: str = ""
    raw: dict = Field(default_factory=dict, exclude=True)


# ── Storage backend ────────────────────────────────────────────────────────────

class StorageBackend(ABC):
    """Pluggable storage for idempotency checks and dead-letter queuing.

    Implement this to persist receipts to your database.
    The default InMemoryStorage is suitable for development and testing only.
    """

    @abstractmethod
    async def receipt_exists(self, receipt: str) -> bool:
        """Return True if this M-Pesa receipt has already been processed."""
        ...

    @abstractmethod
    async def save_receipt(self, receipt: str, payload: dict) -> None:
        """Persist the receipt so future duplicates are detected."""
        ...

    @abstractmethod
    async def push_dead_letter(self, endpoint: str, payload: dict, error: str) -> None:
        """Store a failed callback for later inspection/retry."""
        ...

    @abstractmethod
    async def pop_dead_letters(self) -> list[dict]:
        """Return all dead-lettered payloads (for admin inspection)."""
        ...


class InMemoryStorage(StorageBackend):
    """In-memory storage. Suitable for development and testing only.

    All data is lost on restart. For production, implement StorageBackend
    with your preferred database (PostgreSQL, Redis, Firestore, etc).
    """

    def __init__(self):
        self._receipts: dict[str, dict] = {}
        self._dead_letters: list[dict] = []

    async def receipt_exists(self, receipt: str) -> bool:
        return receipt in self._receipts

    async def save_receipt(self, receipt: str, payload: dict) -> None:
        self._receipts[receipt] = {**payload, "_saved_at": datetime.utcnow().isoformat()}

    async def push_dead_letter(self, endpoint: str, payload: dict, error: str) -> None:
        self._dead_letters.append({
            "endpoint": endpoint,
            "payload": payload,
            "error": error,
            "queued_at": datetime.utcnow().isoformat(),
        })
        logger.error("dead_letter endpoint=%s error=%s", endpoint, error)

    async def pop_dead_letters(self) -> list[dict]:
        items = list(self._dead_letters)
        self._dead_letters.clear()
        return items


# ── HMAC verification ──────────────────────────────────────────────────────────

def _verify_hmac(body: bytes, signature: str, secret: str) -> bool:
    """Verify X-Mpesa-Signature header using HMAC-SHA256."""
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


# ── Router ─────────────────────────────────────────────────────────────────────

HandlerFn = Callable[..., Awaitable[None]]


class MpesaWebhookRouter:
    """Mounts M-Pesa callback endpoints into a FastAPI application.

    Args:
        storage: StorageBackend instance (default: InMemoryStorage)
        hmac_secret: If provided, validates X-Mpesa-Signature header on all requests
        safaricom_ips: If provided, rejects requests from other IPs
                       (Safaricom production IPs: 196.201.214.200/201/202/203,
                        196.201.214.206/207, 196.201.213.114, 196.201.214.207)
    """

    def __init__(
        self,
        storage: Optional[StorageBackend] = None,
        hmac_secret: Optional[str] = None,
        safaricom_ips: Optional[list[str]] = None,
    ):
        self._storage = storage or InMemoryStorage()
        self._hmac_secret = hmac_secret
        self._safaricom_ips = set(safaricom_ips) if safaricom_ips else None
        self._stk_success_handlers: list[HandlerFn] = []
        self._stk_failure_handlers: list[HandlerFn] = []
        self._b2c_result_handlers: list[HandlerFn] = []
        self._c2b_confirmation_handlers: list[HandlerFn] = []
        self.router = APIRouter()
        self._register_routes()

    # ── Decorators ──────────────────────────────────────────────

    def on_stk_success(self, fn: HandlerFn) -> HandlerFn:
        """Register a handler called on successful STK Push payment."""
        self._stk_success_handlers.append(fn)
        return fn

    def on_stk_failure(self, fn: HandlerFn) -> HandlerFn:
        """Register a handler called when STK Push is cancelled/fails."""
        self._stk_failure_handlers.append(fn)
        return fn

    def on_b2c_result(self, fn: HandlerFn) -> HandlerFn:
        """Register a handler called on B2C disbursement result."""
        self._b2c_result_handlers.append(fn)
        return fn

    def on_c2b_confirmation(self, fn: HandlerFn) -> HandlerFn:
        """Register a handler called on confirmed C2B payment."""
        self._c2b_confirmation_handlers.append(fn)
        return fn

    # ── Verification ────────────────────────────────────────────

    async def _verify(self, request: Request) -> bytes:
        body = await request.body()
        if self._safaricom_ips:
            client_ip = request.client.host if request.client else ""
            if client_ip not in self._safaricom_ips:
                logger.warning("rejected_ip ip=%s", client_ip)
                raise HTTPException(403, "Forbidden")
        if self._hmac_secret:
            sig = request.headers.get("X-Mpesa-Signature", "")
            if not _verify_hmac(body, sig, self._hmac_secret):
                logger.warning("invalid_hmac")
                raise HTTPException(401, "Invalid signature")
        return body

    async def _run_handlers(
        self,
        handlers: list[HandlerFn],
        event: Any,
        endpoint: str,
        raw: dict,
    ) -> None:
        for handler in handlers:
            try:
                await handler(event)
            except Exception as exc:
                await self._storage.push_dead_letter(endpoint, raw, str(exc))

    # ── Routes ──────────────────────────────────────────────────

    def _register_routes(self) -> None:
        router = self.router

        @router.post("/stk/callback", include_in_schema=True)
        async def stk_callback(request: Request) -> Response:
            body = await self._verify(request)
            raw = json.loads(body)

            stk = raw.get("Body", {}).get("stkCallback", {})
            result_code = stk.get("ResultCode")
            checkout_id = stk.get("CheckoutRequestID", "")
            merchant_id = stk.get("MerchantRequestID", "")

            logger.info(
                "stk_callback checkout_id=%s result_code=%s",
                checkout_id, result_code,
            )

            if result_code == 0:
                meta = {
                    item["Name"]: item.get("Value")
                    for item in stk.get("CallbackMetadata", {}).get("Item", [])
                }
                receipt = str(meta.get("MpesaReceiptNumber", ""))

                # Idempotency check
                if receipt and await self._storage.receipt_exists(receipt):
                    logger.info("duplicate_receipt receipt=%s — acking", receipt)
                    return _ack()

                await self._storage.save_receipt(receipt, raw)

                raw_date = meta.get("TransactionDate", "")
                try:
                    txn_date = datetime.strptime(str(raw_date), "%Y%m%d%H%M%S")
                except (ValueError, TypeError):
                    txn_date = datetime.utcnow()

                event = STKSuccessEvent(
                    merchant_request_id=merchant_id,
                    checkout_request_id=checkout_id,
                    receipt=receipt,
                    amount=float(meta.get("Amount", 0)),
                    phone=str(meta.get("PhoneNumber", "")),
                    transaction_date=txn_date,
                    raw=raw,
                )
                await self._run_handlers(self._stk_success_handlers, event, "/stk/callback", raw)
            else:
                event = STKFailureEvent(
                    merchant_request_id=merchant_id,
                    checkout_request_id=checkout_id,
                    result_code=int(result_code),
                    result_desc=stk.get("ResultDesc", ""),
                    raw=raw,
                )
                await self._run_handlers(self._stk_failure_handlers, event, "/stk/callback", raw)

            return _ack()

        @router.post("/b2c/result", include_in_schema=True)
        async def b2c_result(request: Request) -> Response:
            body = await self._verify(request)
            raw = json.loads(body)

            result = raw.get("Result", {})
            result_code = int(result.get("ResultCode", -1))
            conv_id = result.get("ConversationID", "")
            orig_id = result.get("OriginatorConversationID", "")

            logger.info("b2c_result conv_id=%s result_code=%s", conv_id, result_code)

            params = {}
            for item in result.get("ResultParameters", {}).get("ResultParameter", []):
                params[item["Key"]] = item.get("Value")

            receipt = str(params.get("TransactionReceipt", ""))
            if receipt and result_code == 0:
                if await self._storage.receipt_exists(receipt):
                    logger.info("duplicate_b2c_receipt receipt=%s", receipt)
                    return _ack()
                await self._storage.save_receipt(receipt, raw)

            event = B2CResultEvent(
                conversation_id=conv_id,
                originator_conversation_id=orig_id,
                result_code=result_code,
                result_desc=result.get("ResultDesc", ""),
                amount=float(params.get("TransactionAmount", 0)) if result_code == 0 else None,
                receipt=receipt or None,
                phone=str(params.get("ReceiverPartyPublicName", "")).split("-")[0].strip() or None,
                raw=raw,
            )
            await self._run_handlers(self._b2c_result_handlers, event, "/b2c/result", raw)
            return _ack()

        @router.post("/b2c/timeout", include_in_schema=True)
        async def b2c_timeout(request: Request) -> Response:
            body = await self._verify(request)
            raw = json.loads(body)
            logger.warning("b2c_timeout raw=%s", raw)
            return _ack()

        @router.post("/c2b/confirmation", include_in_schema=True)
        async def c2b_confirmation(request: Request) -> Response:
            body = await self._verify(request)
            raw = json.loads(body)

            trans_id = raw.get("TransID", "")
            logger.info("c2b_confirmation trans_id=%s", trans_id)

            if await self._storage.receipt_exists(trans_id):
                logger.info("duplicate_c2b trans_id=%s", trans_id)
                return _c2b_accept()

            await self._storage.save_receipt(trans_id, raw)

            event = C2BConfirmationEvent(
                transaction_type=raw.get("TransactionType", ""),
                trans_id=trans_id,
                trans_time=raw.get("TransTime", ""),
                trans_amount=float(raw.get("TransAmount", 0)),
                business_short_code=raw.get("BusinessShortCode", ""),
                bill_ref_number=raw.get("BillRefNumber", ""),
                msisdn=raw.get("MSISDN", ""),
                first_name=raw.get("FirstName", ""),
                last_name=raw.get("LastName", ""),
                raw=raw,
            )
            await self._run_handlers(
                self._c2b_confirmation_handlers, event, "/c2b/confirmation", raw
            )
            return _c2b_accept()

        @router.post("/c2b/validation", include_in_schema=True)
        async def c2b_validation(request: Request) -> Response:
            """C2B validation — always accepts by default.

            Override this endpoint in your app if you need to reject payments
            (e.g. unknown account reference, amount mismatch).
            """
            await self._verify(request)
            return _c2b_accept()

        @router.get("/health", include_in_schema=True)
        async def health() -> dict:
            return {"status": "ok", "service": "mpesa-webhooks", "timestamp": datetime.utcnow().isoformat()}


# ── Response helpers ───────────────────────────────────────────────────────────

def _ack() -> Response:
    """Standard Daraja acknowledgement — must be returned within 5 seconds."""
    return Response(
        content=json.dumps({"ResultCode": 0, "ResultDesc": "Accepted"}),
        media_type="application/json",
        status_code=200,
    )


def _c2b_accept() -> Response:
    return Response(
        content=json.dumps({"ResultCode": 0, "ResultDesc": "Accepted"}),
        media_type="application/json",
        status_code=200,
    )
