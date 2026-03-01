"""mpesa-webhooks test suite."""
from __future__ import annotations

import json
import pytest
from datetime import datetime
from fastapi import FastAPI
from fastapi.testclient import TestClient

from mpesa_webhooks import (
    MpesaWebhookRouter,
    InMemoryStorage,
    STKSuccessEvent,
    STKFailureEvent,
    B2CResultEvent,
    C2BConfirmationEvent,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def storage():
    return InMemoryStorage()

@pytest.fixture
def router(storage):
    return MpesaWebhookRouter(storage=storage)

@pytest.fixture
def app(router):
    app = FastAPI()
    app.include_router(router.router, prefix="/mpesa")
    return app

@pytest.fixture
def client(app):
    return TestClient(app)


# ── Payload builders ────────────────────────────────────────────────────────────

def stk_success_payload(checkout_id="ws_CO_123", receipt="NLJ7RT61SV", amount=100, phone=254712345678):
    return {
        "Body": {
            "stkCallback": {
                "MerchantRequestID": "29115-34620561-1",
                "CheckoutRequestID": checkout_id,
                "ResultCode": 0,
                "ResultDesc": "The service request is processed successfully.",
                "CallbackMetadata": {
                    "Item": [
                        {"Name": "Amount", "Value": amount},
                        {"Name": "MpesaReceiptNumber", "Value": receipt},
                        {"Name": "TransactionDate", "Value": 20240115143022},
                        {"Name": "PhoneNumber", "Value": phone},
                    ]
                }
            }
        }
    }

def stk_failure_payload(checkout_id="ws_CO_456", result_code=1032, desc="Request cancelled by user"):
    return {
        "Body": {
            "stkCallback": {
                "MerchantRequestID": "29115-34620561-2",
                "CheckoutRequestID": checkout_id,
                "ResultCode": result_code,
                "ResultDesc": desc,
            }
        }
    }

def b2c_result_payload(result_code=0, receipt="NLJ7RT61SV", amount=500):
    params = []
    if result_code == 0:
        params = [
            {"Key": "TransactionAmount", "Value": amount},
            {"Key": "TransactionReceipt", "Value": receipt},
            {"Key": "ReceiverPartyPublicName", "Value": "254712345678 - John Doe"},
        ]
    return {
        "Result": {
            "ResultCode": result_code,
            "ResultDesc": "The service request is processed successfully." if result_code == 0 else "Failed",
            "ConversationID": "AG_20240115_1234",
            "OriginatorConversationID": "12345-67890-1",
            "ResultParameters": {"ResultParameter": params},
        }
    }

def c2b_confirmation_payload(trans_id="NLJ7RT61SV", amount=500, ref="INV001"):
    return {
        "TransactionType": "Pay Bill",
        "TransID": trans_id,
        "TransTime": "20240115143022",
        "TransAmount": str(amount),
        "BusinessShortCode": "600000",
        "BillRefNumber": ref,
        "MSISDN": "254712345678",
        "FirstName": "John",
        "LastName": "Doe",
    }


# ── Health ─────────────────────────────────────────────────────────────────────

class TestHealth:
    def test_health_ok(self, client):
        r = client.get("/mpesa/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_health_has_timestamp(self, client):
        r = client.get("/mpesa/health")
        assert "timestamp" in r.json()


# ── STK Success ────────────────────────────────────────────────────────────────

class TestSTKSuccess:
    def test_returns_200(self, client):
        r = client.post("/mpesa/stk/callback", json=stk_success_payload())
        assert r.status_code == 200

    def test_returns_ack(self, client):
        r = client.post("/mpesa/stk/callback", json=stk_success_payload())
        assert r.json()["ResultCode"] == 0

    def test_handler_called(self, router, client):
        received = []

        @router.on_stk_success
        async def handler(event: STKSuccessEvent):
            received.append(event)

        client.post("/mpesa/stk/callback", json=stk_success_payload())
        assert len(received) == 1
        assert received[0].receipt == "NLJ7RT61SV"
        assert received[0].amount == 100
        assert received[0].checkout_request_id == "ws_CO_123"

    def test_phone_captured(self, router, client):
        received = []

        @router.on_stk_success
        async def handler(event: STKSuccessEvent):
            received.append(event)

        client.post("/mpesa/stk/callback", json=stk_success_payload(phone=254787654321))
        assert received[0].phone == "254787654321"

    def test_transaction_date_parsed(self, router, client):
        received = []

        @router.on_stk_success
        async def handler(event: STKSuccessEvent):
            received.append(event)

        client.post("/mpesa/stk/callback", json=stk_success_payload())
        assert isinstance(received[0].transaction_date, datetime)
        assert received[0].transaction_date.year == 2024

    def test_receipt_saved_to_storage(self, client, storage):
        client.post("/mpesa/stk/callback", json=stk_success_payload(receipt="ABC123"))
        import asyncio
        assert asyncio.get_event_loop().run_until_complete(storage.receipt_exists("ABC123"))


class TestSTKIdempotency:
    def test_duplicate_receipt_not_processed_twice(self, router, client):
        received = []

        @router.on_stk_success
        async def handler(event: STKSuccessEvent):
            received.append(event)

        payload = stk_success_payload(receipt="DUPRECEIPT1")
        client.post("/mpesa/stk/callback", json=payload)
        client.post("/mpesa/stk/callback", json=payload)  # duplicate

        assert len(received) == 1  # handler called only once

    def test_duplicate_still_returns_200(self, client):
        payload = stk_success_payload(receipt="DUPRECEIPT2")
        r1 = client.post("/mpesa/stk/callback", json=payload)
        r2 = client.post("/mpesa/stk/callback", json=payload)
        assert r1.status_code == 200
        assert r2.status_code == 200

    def test_different_receipts_both_processed(self, router, client):
        received = []

        @router.on_stk_success
        async def handler(event: STKSuccessEvent):
            received.append(event)

        client.post("/mpesa/stk/callback", json=stk_success_payload(receipt="REC_AAA"))
        client.post("/mpesa/stk/callback", json=stk_success_payload(receipt="REC_BBB"))

        assert len(received) == 2


# ── STK Failure ────────────────────────────────────────────────────────────────

class TestSTKFailure:
    def test_handler_called_on_cancel(self, router, client):
        received = []

        @router.on_stk_failure
        async def handler(event: STKFailureEvent):
            received.append(event)

        client.post("/mpesa/stk/callback", json=stk_failure_payload(result_code=1032))
        assert len(received) == 1
        assert received[0].result_code == 1032

    def test_handler_called_on_timeout(self, router, client):
        received = []

        @router.on_stk_failure
        async def handler(event: STKFailureEvent):
            received.append(event)

        client.post("/mpesa/stk/callback", json=stk_failure_payload(result_code=1037))
        assert received[0].result_code == 1037
        assert received[0].result_desc

    def test_result_desc_captured(self, router, client):
        received = []

        @router.on_stk_failure
        async def handler(event: STKFailureEvent):
            received.append(event)

        client.post("/mpesa/stk/callback", json=stk_failure_payload(
            result_code=1032, desc="Request cancelled by user"
        ))
        assert "cancel" in received[0].result_desc.lower()

    def test_failure_returns_200(self, client):
        r = client.post("/mpesa/stk/callback", json=stk_failure_payload())
        assert r.status_code == 200


# ── B2C Result ─────────────────────────────────────────────────────────────────

class TestB2CResult:
    def test_success_handler_called(self, router, client):
        received = []

        @router.on_b2c_result
        async def handler(event: B2CResultEvent):
            received.append(event)

        client.post("/mpesa/b2c/result", json=b2c_result_payload(receipt="B2C_REC_1"))
        assert len(received) == 1
        assert received[0].succeeded
        assert received[0].receipt == "B2C_REC_1"
        assert received[0].amount == 500

    def test_failure_handler_called(self, router, client):
        received = []

        @router.on_b2c_result
        async def handler(event: B2CResultEvent):
            received.append(event)

        client.post("/mpesa/b2c/result", json=b2c_result_payload(result_code=1))
        assert not received[0].succeeded

    def test_b2c_idempotency(self, router, client):
        received = []

        @router.on_b2c_result
        async def handler(event: B2CResultEvent):
            received.append(event)

        payload = b2c_result_payload(receipt="B2C_DUP_1")
        client.post("/mpesa/b2c/result", json=payload)
        client.post("/mpesa/b2c/result", json=payload)
        assert len(received) == 1

    def test_timeout_returns_200(self, client):
        r = client.post("/mpesa/b2c/timeout", json={"Result": {"ResultCode": 1037}})
        assert r.status_code == 200


# ── C2B Confirmation ───────────────────────────────────────────────────────────

class TestC2BConfirmation:
    def test_handler_called(self, router, client):
        received = []

        @router.on_c2b_confirmation
        async def handler(event: C2BConfirmationEvent):
            received.append(event)

        client.post("/mpesa/c2b/confirmation", json=c2b_confirmation_payload())
        assert len(received) == 1
        assert received[0].trans_amount == 500
        assert received[0].bill_ref_number == "INV001"

    def test_c2b_idempotency(self, router, client):
        received = []

        @router.on_c2b_confirmation
        async def handler(event: C2BConfirmationEvent):
            received.append(event)

        payload = c2b_confirmation_payload(trans_id="C2B_DUP_1")
        client.post("/mpesa/c2b/confirmation", json=payload)
        client.post("/mpesa/c2b/confirmation", json=payload)
        assert len(received) == 1

    def test_validation_accepts(self, client):
        r = client.post("/mpesa/c2b/validation", json=c2b_confirmation_payload())
        assert r.status_code == 200
        assert r.json()["ResultCode"] == 0


# ── Dead-letter queue ──────────────────────────────────────────────────────────

class TestDeadLetter:
    def test_failing_handler_queued(self, router, client, storage):
        @router.on_stk_success
        async def bad_handler(event: STKSuccessEvent):
            raise RuntimeError("DB is down")

        client.post("/mpesa/stk/callback", json=stk_success_payload(receipt="DL_REC_1"))

        import asyncio
        letters = asyncio.get_event_loop().run_until_complete(storage.pop_dead_letters())
        assert len(letters) == 1
        assert "DB is down" in letters[0]["error"]

    def test_failing_handler_still_returns_200(self, router, client):
        @router.on_stk_success
        async def bad_handler(event: STKSuccessEvent):
            raise ValueError("Something broke")

        r = client.post("/mpesa/stk/callback", json=stk_success_payload(receipt="DL_REC_2"))
        assert r.status_code == 200  # Daraja must always get 200 back

    def test_good_handler_runs_after_bad(self, router, client):
        received = []

        @router.on_stk_success
        async def bad_handler(event: STKSuccessEvent):
            raise RuntimeError("first handler fails")

        @router.on_stk_success
        async def good_handler(event: STKSuccessEvent):
            received.append(event)

        client.post("/mpesa/stk/callback", json=stk_success_payload(receipt="DL_REC_3"))
        assert len(received) == 1  # second handler still ran


# ── Multiple handlers ──────────────────────────────────────────────────────────

class TestMultipleHandlers:
    def test_multiple_stk_success_handlers_all_called(self, router, client):
        log_a, log_b = [], []

        @router.on_stk_success
        async def handler_a(event: STKSuccessEvent):
            log_a.append(event.receipt)

        @router.on_stk_success
        async def handler_b(event: STKSuccessEvent):
            log_b.append(event.amount)

        client.post("/mpesa/stk/callback", json=stk_success_payload(receipt="MULTI_1", amount=250))
        assert log_a == ["MULTI_1"]
        assert log_b == [250]


# ── HMAC verification ──────────────────────────────────────────────────────────

class TestHMAC:
    def _make_app(self, secret):
        import hashlib, hmac as _hmac
        router = MpesaWebhookRouter(hmac_secret=secret)
        app = FastAPI()
        app.include_router(router.router, prefix="/mpesa")
        return app, router

    def _sign(self, body: bytes, secret: str) -> str:
        import hashlib, hmac as _hmac
        return _hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

    def test_valid_signature_accepted(self):
        app, _ = self._make_app("supersecret")
        client = TestClient(app)
        payload = stk_success_payload(receipt="HMAC_1")
        body = json.dumps(payload).encode()
        sig = self._sign(body, "supersecret")
        r = client.post("/mpesa/stk/callback", content=body,
                       headers={"Content-Type": "application/json", "X-Mpesa-Signature": sig})
        assert r.status_code == 200

    def test_invalid_signature_rejected(self):
        app, _ = self._make_app("supersecret")
        client = TestClient(app)
        payload = stk_success_payload(receipt="HMAC_2")
        r = client.post("/mpesa/stk/callback", json=payload,
                       headers={"X-Mpesa-Signature": "wrongsig"})
        assert r.status_code == 401
