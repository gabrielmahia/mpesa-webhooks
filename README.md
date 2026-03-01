# mpesa-webhooks

**Production FastAPI handler for M-Pesa Daraja callbacks.**

[![CI](https://github.com/gabrielmahia/mpesa-webhooks/actions/workflows/ci.yml/badge.svg)](https://github.com/gabrielmahia/mpesa-webhooks/actions)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](#)
[![Tests](https://img.shields.io/badge/tests-44%20passing-brightgreen)](#)
[![License](https://img.shields.io/badge/License-CC%20BY--NC--ND%204.0-lightgrey)](LICENSE)

Handles STK Push callbacks, B2C results, and C2B confirmations with production-grade
guarantees out of the box: idempotency, HMAC signature verification, pluggable storage,
and a dead-letter queue so failed handlers never silently drop a payment.

---

## Install

```bash
pip install mpesa-webhooks
# With uvicorn for standalone serving:
pip install "mpesa-webhooks[server]"
```

---

## Quickstart

```python
from fastapi import FastAPI
from mpesa_webhooks import MpesaWebhookRouter, STKSuccessEvent, STKFailureEvent

app = FastAPI()
router = MpesaWebhookRouter()

@router.on_stk_success
async def payment_received(event: STKSuccessEvent):
    print(f"Payment {event.receipt}: KES {event.amount} from {event.phone}")
    await db.orders.mark_paid(event.checkout_request_id, event.receipt)

@router.on_stk_failure
async def payment_failed(event: STKFailureEvent):
    print(f"Payment failed: {event.result_code} — {event.result_desc}")
    await db.orders.mark_failed(event.checkout_request_id)

app.include_router(router.router, prefix="/mpesa")
```

Then register your callback URL in the Daraja portal:
```
https://yourapp.com/mpesa/stk/callback
```

---

## Endpoints

| Endpoint | Handles |
|----------|---------|
| `POST /mpesa/stk/callback` | STK Push success and failure |
| `POST /mpesa/b2c/result` | B2C disbursement result |
| `POST /mpesa/b2c/timeout` | B2C queue timeout |
| `POST /mpesa/c2b/confirmation` | C2B payment confirmed |
| `POST /mpesa/c2b/validation` | C2B payment validation (accepts by default) |
| `GET  /mpesa/health` | Health check |

---

## Events

### `STKSuccessEvent`
```python
event.receipt              # "NLJ7RT61SV" — M-Pesa receipt number
event.amount               # 100.0
event.phone                # "254712345678"
event.checkout_request_id  # "ws_CO_..."
event.transaction_date     # datetime(2024, 1, 15, 14, 30, 22)
```

### `STKFailureEvent`
```python
event.result_code   # 1032 (cancelled), 1037 (timeout), 2001 (wrong PIN)
event.result_desc   # "Request cancelled by user"
event.checkout_request_id
```

### `B2CResultEvent`
```python
event.succeeded    # True / False
event.receipt      # "NLJ7RT61SV"
event.amount       # 500.0
event.phone        # "254712345678"
```

### `C2BConfirmationEvent`
```python
event.trans_id         # "NLJ7RT61SV"
event.trans_amount     # 500.0
event.bill_ref_number  # "INV001" — your account reference
event.msisdn           # "254712345678"
```

---

## Idempotency

Every M-Pesa receipt number is checked against storage before calling handlers.
Duplicate callbacks — which Safaricom sends on retries — are detected and silently
acknowledged without re-running your business logic.

```python
# First delivery: handlers called, receipt saved
POST /mpesa/stk/callback  →  handlers run, receipt "NLJ7RT61SV" saved

# Safaricom retries the same callback 30 seconds later:
POST /mpesa/stk/callback  →  duplicate detected, handlers skipped, 200 returned
```

---

## Dead-letter queue

If a handler raises an exception, the callback is pushed to a dead-letter queue
and the **next handler still runs**. Safaricom always receives HTTP 200. Nothing
is silently dropped.

```python
@router.on_stk_success
async def save_to_db(event: STKSuccessEvent):
    await db.save(event)  # If this raises, goes to dead-letter

@router.on_stk_success
async def send_sms(event: STKSuccessEvent):
    await sms.send(event.phone, f"Payment received: KES {event.amount}")
    # This still runs even if save_to_db failed
```

---

## HMAC signature verification

```python
router = MpesaWebhookRouter(hmac_secret="your-shared-secret")
# Requests without a valid X-Mpesa-Signature header return HTTP 401
```

---

## IP allowlist

```python
router = MpesaWebhookRouter(
    safaricom_ips=[
        "196.201.214.200", "196.201.214.201", "196.201.214.202", "196.201.214.203",
        "196.201.214.206", "196.201.214.207", "196.201.213.114",
    ]
)
```

---

## Custom storage backend

The default `InMemoryStorage` is for development only — data is lost on restart.
For production, implement `StorageBackend`:

```python
from mpesa_webhooks import StorageBackend, MpesaWebhookRouter

class PostgresStorage(StorageBackend):
    async def receipt_exists(self, receipt: str) -> bool:
        return await db.fetchval("SELECT 1 FROM receipts WHERE receipt = $1", receipt)

    async def save_receipt(self, receipt: str, payload: dict) -> None:
        await db.execute("INSERT INTO receipts (receipt, payload) VALUES ($1, $2)",
                        receipt, json.dumps(payload))

    async def push_dead_letter(self, endpoint: str, payload: dict, error: str) -> None:
        await db.execute("INSERT INTO dead_letters (endpoint, payload, error) VALUES ($1, $2, $3)",
                        endpoint, json.dumps(payload), error)

    async def pop_dead_letters(self) -> list[dict]:
        rows = await db.fetch("DELETE FROM dead_letters RETURNING *")
        return [dict(r) for r in rows]

router = MpesaWebhookRouter(storage=PostgresStorage())
```

---

## Testing with daraja-mock

```python
from daraja_mock import DarajaMock, Scenario
from fastapi.testclient import TestClient

def test_payment_flow():
    mock = DarajaMock()
    received = []

    router = MpesaWebhookRouter()

    @router.on_stk_success
    async def handler(event):
        received.append(event)

    app = FastAPI()
    app.include_router(router.router, prefix="/mpesa")
    client = TestClient(app)

    # Simulate Safaricom posting the callback
    callback = mock.build_stk_callback(scenario=Scenario.SUCCESS)
    r = client.post("/mpesa/stk/callback", json=callback)

    assert r.status_code == 200
    assert len(received) == 1
    assert received[0].amount == 100
```

---

*Part of the [nairobi-stack](https://github.com/gabrielmahia/nairobi-stack) East Africa engineering ecosystem.*
*Maintained by [Gabriel Mahia](https://github.com/gabrielmahia). Kenya × USA.*
