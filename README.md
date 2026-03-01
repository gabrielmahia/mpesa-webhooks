# mpesa-webhooks

**Production FastAPI handler for M-Pesa Daraja callbacks. Idempotent. Dead-letter queue. Pluggable storage.**

[![CI](https://github.com/gabrielmahia/mpesa-webhooks/actions/workflows/ci.yml/badge.svg)](https://github.com/gabrielmahia/mpesa-webhooks/actions)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](#)
[![Tests](https://img.shields.io/badge/tests-42%20passing-brightgreen)](#)
[![License](https://img.shields.io/badge/License-CC%20BY--NC--ND%204.0-lightgrey)](LICENSE)

The hardest part of M-Pesa integration is not the STK Push — it is handling the callback correctly.
Safaricom will retry. Your server will restart mid-payment. The same receipt will arrive twice.
This library handles all of that so your application code does not have to.

---

## Install

```bash
pip install mpesa-webhooks
```

---

## Mount in your FastAPI app

```python
from fastapi import FastAPI
from mpesa_webhooks.router import build_router, WebhookConfig
from mpesa_webhooks.models import StkPushCallback, B2CResultCallback

async def on_payment_received(cb: StkPushCallback) -> None:
    await db.record_payment(
        receipt=cb.mpesa_receipt,
        amount=cb.amount,
        phone=cb.phone_number,
        checkout_id=cb.checkout_request_id,
    )

async def on_payment_failed(cb: StkPushCallback) -> None:
    await db.mark_payment_failed(
        checkout_id=cb.checkout_request_id,
        reason=cb.result_desc,
    )

config = WebhookConfig(
    on_stk_success=on_payment_received,
    on_stk_failure=on_payment_failed,
)

app = FastAPI()
app.include_router(build_router(config), prefix="/mpesa")
```

Register with Safaricom:
- STK Push callback URL: `https://yourdomain.com/mpesa/stk/callback`
- B2C result URL: `https://yourdomain.com/mpesa/b2c/result`
- C2B confirmation URL: `https://yourdomain.com/mpesa/c2b/confirmation`

---

## Why this library exists

Three things break every M-Pesa webhook integration in production:

**1. Duplicate callbacks.** Safaricom retries on timeout. If your `/callback` takes >5s, you will
receive the same payment notification multiple times. Without idempotency, you record the payment twice.

```python
# Built-in: checkout_request_id is your idempotency key
# The router deduplicates automatically — your handler is called exactly once
```

**2. Exceptions swallowed by Safaricom retries.** If your handler raises and you return HTTP 500,
Safaricom retries. If your DB is down for 2 minutes, you get 20 duplicate attempts.

```python
# Built-in: handler exceptions are caught, written to DLQ, still return 200
# Your retry logic runs on your schedule, not Safaricom's
```

**3. Parsing the callback is fragile.** The `CallbackMetadata.Item` array is a list of `{"Name": ..., "Value": ...}`
objects, not a dict. The `TransactionDate` is an integer formatted as `YYYYMMDDHHmmss`. `PhoneNumber`
is a long, not a string. Getting this wrong causes silent data loss.

```python
# Built-in: StkPushCallback.from_daraja() handles all of this
cb = StkPushCallback.from_daraja(payload)
cb.mpesa_receipt     # str | None
cb.transaction_date  # datetime | None (properly parsed)
cb.phone_number      # "254712345678" as str
```

---

## Idempotency

```python
from mpesa_webhooks.idempotency import IdempotencyChecker, InMemoryIdempotencyStore

# Default: in-memory (single process)
checker = IdempotencyChecker()

# Production (multi-replica): plug in Redis
import redis
class RedisStore:
    def __init__(self): self.r = redis.Redis()
    def exists(self, key): return bool(self.r.exists(key))
    def mark(self, key, ttl_seconds=86400): self.r.setex(key, ttl_seconds, 1)

checker = IdempotencyChecker(store=RedisStore())
config = WebhookConfig(on_stk_success=handler, idempotency=checker)
```

---

## Dead-letter queue

```python
from mpesa_webhooks.dlq import InMemoryDLQ

dlq = InMemoryDLQ(max_attempts=5)
config = WebhookConfig(on_stk_success=handler, dlq=dlq)

# Background worker — retry every 30s
import asyncio
async def retry_worker():
    while True:
        for letter in dlq.pending():
            try:
                cb = StkPushCallback.from_daraja(letter.raw_payload)
                await handler(cb)
                dlq.remove(letter)
            except Exception as exc:
                letter.record_retry_failure(str(exc), dlq.backoff_seconds(letter.attempts))
        await asyncio.sleep(30)
```

---

## Parsed models

### StkPushCallback
| Field | Type | Notes |
|-------|------|-------|
| `checkout_request_id` | `str` | Idempotency key |
| `result_code` | `int` | 0 = success |
| `amount` | `float \| None` | Success only |
| `mpesa_receipt` | `str \| None` | Success only |
| `phone_number` | `str \| None` | E.164 format |
| `transaction_date` | `datetime \| None` | Parsed from Safaricom integer |
| `.succeeded` | `bool` | `result_code == 0` |
| `.user_cancelled` | `bool` | `result_code == 1032` |
| `.insufficient_funds` | `bool` | `result_code == 1` |

### B2CResultCallback
| Field | Type |
|-------|------|
| `conversation_id` | `str` |
| `transaction_id` | `str` |
| `transaction_amount` | `float \| None` |
| `receiver_phone` | `str \| None` |

---

## Health check

```
GET /mpesa/health
→ {"status": "ok", "dlq_pending": 0, "dlq_exhausted": 0, "ts": 1234567890.0}
```

---

## Used with daraja-mock in tests

```python
from daraja_mock import DarajaMock, Scenario

def test_duplicate_payment_handled_once():
    mock = DarajaMock()
    received = []
    def handler(cb): received.append(cb)

    config = WebhookConfig(on_stk_success=handler)
    app = FastAPI()
    app.include_router(build_router(config), prefix="/mpesa")

    with TestClient(app) as client:
        payload = mock.build_stk_callback(checkout_request_id="ws_CO_001")
        client.post("/mpesa/stk/callback", json=payload)
        client.post("/mpesa/stk/callback", json=payload)  # Safaricom retry

    assert len(received) == 1  # handled exactly once
```

---

*Maintained by [Gabriel Mahia](https://github.com/gabrielmahia). Kenya × USA.*
*Part of the [East Africa fintech toolkit](https://github.com/gabrielmahia/nairobi-stack).*
