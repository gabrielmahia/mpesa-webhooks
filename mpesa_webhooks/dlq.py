"""
mpesa_webhooks.dlq — Dead-letter queue for failed callback processing.

When your handler raises an exception (database down, downstream
service unavailable, etc.), the callback should NOT return a non-200
to Daraja — Safaricom will keep retrying with exponential backoff,
flooding your endpoint.

The correct pattern:
1. Accept the callback (return 200 immediately)
2. Attempt processing
3. On failure, write to dead-letter queue
4. Background job retries DLQ entries

This module provides the queue and retry machinery. Storage is
pluggable — swap InMemoryDLQ for a database-backed implementation.

Usage::

    dlq = InMemoryDLQ()

    try:
        process(callback)
    except Exception as exc:
        dlq.push(DeadLetter(
            callback_type="stk_push",
            idempotency_key=f"stk:{callback.checkout_request_id}",
            raw_payload=raw,
            error=str(exc),
        ))
        # Still return 200 to Daraja
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DeadLetter:
    """A failed callback waiting for retry."""
    callback_type:    str             # "stk_push", "b2c_result", "c2b_payment"
    idempotency_key:  str
    raw_payload:      dict[str, Any]
    error:            str             # Exception message
    attempts:         int  = 0
    first_failed_at:  float = field(default_factory=time.time)
    last_failed_at:   float = field(default_factory=time.time)
    next_retry_at:    float = field(default_factory=time.time)

    def record_retry_failure(self, error: str, backoff_seconds: float) -> None:
        self.attempts += 1
        self.error = error
        self.last_failed_at = time.time()
        self.next_retry_at = time.time() + backoff_seconds


class InMemoryDLQ:
    """Thread-safe in-memory dead-letter queue.

    For production: replace with a Postgres table, Redis list, or
    SQS dead-letter queue.
    """

    def __init__(self, max_attempts: int = 5) -> None:
        self.max_attempts = max_attempts
        self._queue: list[DeadLetter] = []
        self._lock = threading.Lock()

    def push(self, letter: DeadLetter) -> None:
        """Add a failed callback to the queue."""
        with self._lock:
            self._queue.append(letter)

    def pending(self) -> list[DeadLetter]:
        """Items ready for retry (next_retry_at <= now, attempts < max)."""
        now = time.time()
        with self._lock:
            return [
                l for l in self._queue
                if l.next_retry_at <= now and l.attempts < self.max_attempts
            ]

    def exhausted(self) -> list[DeadLetter]:
        """Items that have exceeded max_attempts — need manual intervention."""
        with self._lock:
            return [l for l in self._queue if l.attempts >= self.max_attempts]

    def remove(self, letter: DeadLetter) -> None:
        with self._lock:
            try:
                self._queue.remove(letter)
            except ValueError:
                pass

    def clear(self) -> None:
        with self._lock:
            self._queue.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._queue)

    def backoff_seconds(self, attempts: int) -> float:
        """Exponential backoff: 30s, 60s, 120s, 240s, 480s."""
        return min(30 * (2 ** attempts), 480)
