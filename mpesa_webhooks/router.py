"""
mpesa_webhooks.router — FastAPI router for all M-Pesa Daraja callbacks.

Mount this router in your existing FastAPI application:

    from mpesa_webhooks.router import build_router, WebhookConfig

    config = WebhookConfig(
        on_stk_success=handle_payment,
        on_stk_failure=handle_failure,
        on_b2c_result=handle_payout,
        on_c2b_payment=handle_paybill,
    )
    app.include_router(build_router(config), prefix="/mpesa")

Then Safaricom POSTs to:
    /mpesa/stk/callback
    /mpesa/b2c/result
    /mpesa/b2c/timeout
    /mpesa/c2b/validation
    /mpesa/c2b/confirmation

Design decisions:
- ALWAYS return HTTP 200 to Daraja — even on processing failure.
  Non-200 causes Safaricom to retry, flooding your endpoint.
- Idempotency checked before calling your handler.
- Exceptions caught, logged, and written to DLQ — never propagated.
- All handlers are optional — unregistered callbacks are ack'd silently.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from .dlq import DeadLetter, InMemoryDLQ
from .idempotency import IdempotencyChecker, InMemoryIdempotencyStore
from .models import (
    B2CResultCallback,
    C2BPaymentCallback,
    StkPushCallback,
    detect_callback_type,
)

logger = logging.getLogger("mpesa_webhooks")

StkHandler = Callable[[StkPushCallback], Awaitable[None] | None]
B2CHandler = Callable[[B2CResultCallback], Awaitable[None] | None]
C2BHandler = Callable[[C2BPaymentCallback], Awaitable[None] | None]

_ACK = JSONResponse({"ResultCode": 0, "ResultDesc": "Accepted"})
_ACK_C2B = JSONResponse({"ResultCode": 0, "ResultDesc": "Accepted"})


@dataclass
class WebhookConfig:
    """Handler registry. All fields optional — unhandled callbacks are ack'd silently."""

    on_stk_success:  StkHandler | None = None
    on_stk_failure:  StkHandler | None = None
    on_b2c_result:   B2CHandler | None = None
    on_c2b_payment:  C2BHandler | None = None
    idempotency:     IdempotencyChecker | None = None
    dlq:             InMemoryDLQ       | None = None
    log_raw:         bool = False   # Set True to log raw payloads (use only in dev)


async def _invoke(handler: Any, arg: Any) -> None:
    import inspect
    if inspect.iscoroutinefunction(handler):
        await handler(arg)
    elif handler:
        handler(arg)


def build_router(config: WebhookConfig | None = None) -> APIRouter:
    """Build and return the webhook router with the given config.

    Call with no args to get a no-op router that accepts all callbacks
    and returns 200. Useful for testing that your URL registration works.
    """
    cfg = config or WebhookConfig()
    checker = cfg.idempotency or IdempotencyChecker(InMemoryIdempotencyStore())
    dlq = cfg.dlq or InMemoryDLQ()

    router = APIRouter(tags=["M-Pesa Webhooks"])

    @router.post("/stk/callback")
    async def stk_callback(request: Request) -> Response:
        raw: dict[str, Any] = await request.json()
        if cfg.log_raw:
            logger.debug("STK callback raw: %s", raw)

        try:
            cb = StkPushCallback.from_daraja(raw)
        except (ValueError, KeyError) as exc:
            logger.warning("Unparseable STK callback: %s | payload: %s", exc, raw)
            return _ACK

        key = f"stk:{cb.checkout_request_id}"
        if checker.check_and_mark(key):
            logger.info("Duplicate STK callback ignored: %s", cb.checkout_request_id)
            return _ACK

        handler = cfg.on_stk_success if cb.succeeded else cfg.on_stk_failure
        if handler:
            try:
                await _invoke(handler, cb)
            except Exception as exc:
                logger.exception("STK handler raised: %s", exc)
                dlq.push(DeadLetter(
                    callback_type="stk_push",
                    idempotency_key=key,
                    raw_payload=raw,
                    error=str(exc),
                ))

        return _ACK

    @router.post("/b2c/result")
    async def b2c_result(request: Request) -> Response:
        raw = await request.json()
        if cfg.log_raw:
            logger.debug("B2C result raw: %s", raw)

        try:
            cb = B2CResultCallback.from_daraja(raw)
        except (ValueError, KeyError) as exc:
            logger.warning("Unparseable B2C callback: %s", exc)
            return _ACK

        key = f"b2c:{cb.transaction_id or cb.conversation_id}"
        if checker.check_and_mark(key):
            logger.info("Duplicate B2C callback ignored: %s", key)
            return _ACK

        if cfg.on_b2c_result:
            try:
                await _invoke(cfg.on_b2c_result, cb)
            except Exception as exc:
                logger.exception("B2C handler raised: %s", exc)
                dlq.push(DeadLetter("b2c_result", key, raw, str(exc)))

        return _ACK

    @router.post("/b2c/timeout")
    async def b2c_timeout(request: Request) -> Response:
        raw = await request.json()
        logger.warning("B2C timeout received: %s", raw)
        return _ACK

    @router.post("/c2b/validation")
    async def c2b_validation(request: Request) -> Response:
        # Return 0 to accept all incoming payments.
        # Override this endpoint if you need payment validation logic.
        return _ACK_C2B

    @router.post("/c2b/confirmation")
    async def c2b_confirmation(request: Request) -> Response:
        raw = await request.json()
        if cfg.log_raw:
            logger.debug("C2B confirmation raw: %s", raw)

        try:
            cb = C2BPaymentCallback.from_daraja(raw)
        except (ValueError, KeyError) as exc:
            logger.warning("Unparseable C2B callback: %s", exc)
            return _ACK_C2B

        key = f"c2b:{cb.trans_id}"
        if checker.check_and_mark(key):
            return _ACK_C2B

        if cfg.on_c2b_payment:
            try:
                await _invoke(cfg.on_c2b_payment, cb)
            except Exception as exc:
                logger.exception("C2B handler raised: %s", exc)
                dlq.push(DeadLetter("c2b_payment", key, raw, str(exc)))

        return _ACK_C2B

    @router.get("/health")
    async def health() -> dict:
        return {
            "status": "ok",
            "dlq_pending": len(dlq.pending()),
            "dlq_exhausted": len(dlq.exhausted()),
            "ts": time.time(),
        }

    return router
