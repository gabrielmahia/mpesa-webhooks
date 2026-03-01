"""mpesa-webhooks test suite."""
from __future__ import annotations

import pytest
from datetime import datetime

from mpesa_webhooks.models import (
    StkPushCallback, B2CResultCallback, C2BPaymentCallback,
    detect_callback_type, CallbackType,
)
from mpesa_webhooks.idempotency import InMemoryIdempotencyStore, IdempotencyChecker
from mpesa_webhooks.dlq import InMemoryDLQ, DeadLetter


# ── Fixtures ───────────────────────────────────────────────────────────────────

def stk_success_payload(checkout_id="ws_CO_123"):
    return {
        "Body": {
            "stkCallback": {
                "MerchantRequestID": "29115-1234",
                "CheckoutRequestID": checkout_id,
                "ResultCode": 0,
                "ResultDesc": "The service request is processed successfully.",
                "CallbackMetadata": {
                    "Item": [
                        {"Name": "Amount", "Value": 100},
                        {"Name": "MpesaReceiptNumber", "Value": "NLJ7RT61SV"},
                        {"Name": "TransactionDate", "Value": 20240101120000},
                        {"Name": "PhoneNumber", "Value": 254712345678},
                    ]
                },
            }
        }
    }

def stk_failure_payload(result_code=1032, checkout_id="ws_CO_456"):
    return {
        "Body": {
            "stkCallback": {
                "MerchantRequestID": "29115-5678",
                "CheckoutRequestID": checkout_id,
                "ResultCode": result_code,
                "ResultDesc": "Request cancelled by user",
            }
        }
    }

def b2c_payload(conv_id="AG_001", txn_id="LGR9193RY5"):
    return {
        "Result": {
            "ResultType": 0,
            "ResultCode": 0,
            "ResultDesc": "The service request is processed successfully.",
            "OriginatorConversationID": "ORIG-001",
            "ConversationID": conv_id,
            "TransactionID": txn_id,
            "ResultParameters": {
                "ResultParameter": [
                    {"Key": "TransactionAmount", "Value": 500},
                    {"Key": "ReceiverPartyPublicName", "Value": "254712345678 - Jane Wanjiku"},
                ]
            }
        }
    }

def c2b_payload():
    return {
        "TransactionType": "Pay Bill",
        "TransID": "LGR019G3J2",
        "TransTime": "20191122063845",
        "TransAmount": "10",
        "BusinessShortCode": "600638",
        "BillRefNumber": "account",
        "InvoiceNumber": "",
        "OrgAccountBalance": "",
        "ThirdPartyTransID": "",
        "MSISDN": "254708374149",
        "FirstName": "John",
        "MiddleName": "",
        "LastName": "Doe",
    }


# ── StkPushCallback ────────────────────────────────────────────────────────────

class TestStkPushCallback:
    def test_success_parse(self):
        cb = StkPushCallback.from_daraja(stk_success_payload())
        assert cb.succeeded
        assert cb.result_code == 0
        assert cb.amount == 100.0
        assert cb.mpesa_receipt == "NLJ7RT61SV"
        assert cb.phone_number == "254712345678"

    def test_success_transaction_date_parsed(self):
        cb = StkPushCallback.from_daraja(stk_success_payload())
        assert isinstance(cb.transaction_date, datetime)
        assert cb.transaction_date.year == 2024

    def test_user_cancelled(self):
        cb = StkPushCallback.from_daraja(stk_failure_payload(1032))
        assert not cb.succeeded
        assert cb.user_cancelled
        assert cb.amount is None
        assert cb.mpesa_receipt is None

    def test_insufficient_funds(self):
        cb = StkPushCallback.from_daraja(stk_failure_payload(1))
        assert cb.insufficient_funds

    def test_failure_has_no_metadata(self):
        cb = StkPushCallback.from_daraja(stk_failure_payload())
        assert cb.amount is None
        assert cb.mpesa_receipt is None
        assert cb.transaction_date is None

    def test_checkout_id_preserved(self):
        cb = StkPushCallback.from_daraja(stk_success_payload("ws_CO_TEST"))
        assert cb.checkout_request_id == "ws_CO_TEST"

    def test_invalid_payload_raises(self):
        with pytest.raises(ValueError):
            StkPushCallback.from_daraja({"wrong": "structure"})

    def test_missing_body_raises(self):
        with pytest.raises(ValueError):
            StkPushCallback.from_daraja({})


# ── B2CResultCallback ──────────────────────────────────────────────────────────

class TestB2CResultCallback:
    def test_success_parse(self):
        cb = B2CResultCallback.from_daraja(b2c_payload())
        assert cb.succeeded
        assert cb.transaction_amount == 500.0
        assert cb.conversation_id == "AG_001"
        assert cb.transaction_id == "LGR9193RY5"

    def test_receiver_phone_extracted(self):
        cb = B2CResultCallback.from_daraja(b2c_payload())
        assert cb.receiver_phone == "254712345678"

    def test_invalid_payload_raises(self):
        with pytest.raises(ValueError):
            B2CResultCallback.from_daraja({"no": "result"})


# ── C2BPaymentCallback ─────────────────────────────────────────────────────────

class TestC2BPaymentCallback:
    def test_parse(self):
        cb = C2BPaymentCallback.from_daraja(c2b_payload())
        assert cb.trans_id == "LGR019G3J2"
        assert cb.trans_amount == 10.0
        assert cb.msisdn == "254708374149"
        assert cb.first_name == "John"

    def test_missing_required_key_raises(self):
        bad = c2b_payload()
        del bad["TransID"]
        with pytest.raises(ValueError):
            C2BPaymentCallback.from_daraja(bad)


# ── detect_callback_type ───────────────────────────────────────────────────────

class TestDetectCallbackType:
    def test_detects_stk(self):
        assert detect_callback_type(stk_success_payload()) == CallbackType.STK_PUSH

    def test_detects_b2c(self):
        assert detect_callback_type(b2c_payload()) == CallbackType.B2C_RESULT

    def test_detects_c2b(self):
        assert detect_callback_type(c2b_payload()) == CallbackType.C2B_PAYMENT

    def test_unknown(self):
        assert detect_callback_type({"random": "data"}) == CallbackType.UNKNOWN


# ── IdempotencyChecker ─────────────────────────────────────────────────────────

class TestIdempotency:
    def test_first_call_not_duplicate(self):
        checker = IdempotencyChecker()
        assert not checker.is_duplicate("stk:abc")

    def test_after_mark_is_duplicate(self):
        checker = IdempotencyChecker()
        checker.mark_processed("stk:abc")
        assert checker.is_duplicate("stk:abc")

    def test_check_and_mark_first_call_returns_false(self):
        checker = IdempotencyChecker()
        assert not checker.check_and_mark("stk:xyz")

    def test_check_and_mark_second_call_returns_true(self):
        checker = IdempotencyChecker()
        checker.check_and_mark("stk:xyz")
        assert checker.check_and_mark("stk:xyz")

    def test_different_keys_independent(self):
        checker = IdempotencyChecker()
        checker.mark_processed("stk:one")
        assert not checker.is_duplicate("stk:two")

    def test_expired_key_not_duplicate(self):
        import time
        store = InMemoryIdempotencyStore()
        store.mark("stk:old", ttl_seconds=0)
        time.sleep(0.01)
        assert not store.exists("stk:old")

    def test_store_len(self):
        store = InMemoryIdempotencyStore()
        store.mark("k1")
        store.mark("k2")
        assert len(store) == 2


# ── InMemoryDLQ ────────────────────────────────────────────────────────────────

class TestDLQ:
    def _letter(self, key="stk:fail"):
        return DeadLetter(
            callback_type="stk_push",
            idempotency_key=key,
            raw_payload={"test": True},
            error="DB connection failed",
        )

    def test_push_and_pending(self):
        dlq = InMemoryDLQ()
        dlq.push(self._letter())
        assert len(dlq.pending()) == 1

    def test_max_attempts_exhausted(self):
        dlq = InMemoryDLQ(max_attempts=3)
        letter = self._letter()
        letter.attempts = 3
        dlq.push(letter)
        assert len(dlq.pending()) == 0
        assert len(dlq.exhausted()) == 1

    def test_remove(self):
        dlq = InMemoryDLQ()
        letter = self._letter()
        dlq.push(letter)
        dlq.remove(letter)
        assert len(dlq) == 0

    def test_clear(self):
        dlq = InMemoryDLQ()
        dlq.push(self._letter("k1"))
        dlq.push(self._letter("k2"))
        dlq.clear()
        assert len(dlq) == 0

    def test_backoff_increases(self):
        dlq = InMemoryDLQ()
        assert dlq.backoff_seconds(0) < dlq.backoff_seconds(1)
        assert dlq.backoff_seconds(1) < dlq.backoff_seconds(2)

    def test_backoff_caps_at_480(self):
        dlq = InMemoryDLQ()
        assert dlq.backoff_seconds(99) == 480


# ── FastAPI router integration ─────────────────────────────────────────────────

class TestRouter:
    def _app(self, config=None):
        from fastapi import FastAPI
        from mpesa_webhooks.router import build_router
        app = FastAPI()
        app.include_router(build_router(config), prefix="/mpesa")
        return app

    def _client(self, app):
        from fastapi.testclient import TestClient
        return TestClient(app)

    def test_stk_success_returns_200(self):
        client = self._client(self._app())
        r = client.post("/mpesa/stk/callback", json=stk_success_payload())
        assert r.status_code == 200
        assert r.json()["ResultCode"] == 0

    def test_stk_failure_returns_200(self):
        client = self._client(self._app())
        r = client.post("/mpesa/stk/callback", json=stk_failure_payload())
        assert r.status_code == 200

    def test_b2c_result_returns_200(self):
        client = self._client(self._app())
        r = client.post("/mpesa/b2c/result", json=b2c_payload())
        assert r.status_code == 200

    def test_c2b_confirmation_returns_200(self):
        client = self._client(self._app())
        r = client.post("/mpesa/c2b/confirmation", json=c2b_payload())
        assert r.status_code == 200

    def test_c2b_validation_returns_200(self):
        client = self._client(self._app())
        r = client.post("/mpesa/c2b/validation", json={})
        assert r.status_code == 200

    def test_health_endpoint(self):
        client = self._client(self._app())
        r = client.get("/mpesa/health")
        assert r.status_code == 200
        data = r.json()
        assert "status" in data
        assert data["status"] == "ok"

    def test_stk_handler_called_on_success(self):
        from mpesa_webhooks.router import WebhookConfig

        received = []
        def handler(cb): received.append(cb)

        config = WebhookConfig(on_stk_success=handler)
        client = self._client(self._app(config))
        client.post("/mpesa/stk/callback", json=stk_success_payload())
        assert len(received) == 1
        assert received[0].mpesa_receipt == "NLJ7RT61SV"

    def test_failure_handler_called_on_cancellation(self):
        from mpesa_webhooks.router import WebhookConfig

        received = []
        def handler(cb): received.append(cb)

        config = WebhookConfig(on_stk_failure=handler)
        client = self._client(self._app(config))
        client.post("/mpesa/stk/callback", json=stk_failure_payload(1032))
        assert len(received) == 1
        assert received[0].user_cancelled

    def test_duplicate_stk_callback_not_handled_twice(self):
        from mpesa_webhooks.router import WebhookConfig

        received = []
        def handler(cb): received.append(cb)

        config = WebhookConfig(on_stk_success=handler)
        client = self._client(self._app(config))
        payload = stk_success_payload("ws_CO_DEDUP_TEST")
        client.post("/mpesa/stk/callback", json=payload)
        client.post("/mpesa/stk/callback", json=payload)  # duplicate
        assert len(received) == 1  # handler called exactly once

    def test_handler_exception_does_not_raise_500(self):
        from mpesa_webhooks.router import WebhookConfig

        def bad_handler(cb):
            raise RuntimeError("DB is down")

        config = WebhookConfig(on_stk_success=bad_handler)
        client = self._client(self._app(config))
        r = client.post("/mpesa/stk/callback", json=stk_success_payload())
        assert r.status_code == 200  # still 200 — DLQ absorbed the error

    def test_failed_handler_writes_to_dlq(self):
        from mpesa_webhooks.router import WebhookConfig
        from mpesa_webhooks.dlq import InMemoryDLQ

        dlq = InMemoryDLQ()

        def bad_handler(cb):
            raise RuntimeError("DB is down")

        config = WebhookConfig(on_stk_success=bad_handler, dlq=dlq)
        client = self._client(self._app(config))
        client.post("/mpesa/stk/callback", json=stk_success_payload())
        assert len(dlq) == 1
        assert dlq.pending()[0].error == "DB is down"

    def test_malformed_stk_payload_returns_200(self):
        client = self._client(self._app())
        r = client.post("/mpesa/stk/callback", json={"garbage": True})
        assert r.status_code == 200  # never 4xx to Daraja
