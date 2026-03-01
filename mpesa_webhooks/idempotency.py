"""
mpesa_webhooks.idempotency — Duplicate callback detection.

M-Pesa callbacks are not guaranteed to be delivered exactly once.
Safaricom may retry on timeout, and ngrok tunnels can duplicate
deliveries in development. This module provides a pluggable
idempotency store so your handler can safely process callbacks
exactly once.

Default: in-memory (fine for single-process, not for multi-replica).
Plug in: any dict-like with get/set semantics (Redis, DynamoDB, etc.)

Usage::

    store = InMemoryIdempotencyStore()
    checker = IdempotencyChecker(store)

    # In your webhook handler:
    key = f"stk:{callback.checkout_request_id}"
    if checker.is_duplicate(key):
        return {"status": "duplicate", "idempotent": True}
    checker.mark_processed(key)
    # ... process ...
"""
from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod


class IdempotencyStore(ABC):
    """Abstract interface for idempotency backends."""

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def mark(self, key: str, ttl_seconds: int = 86400) -> None: ...


class InMemoryIdempotencyStore(IdempotencyStore):
    """Thread-safe in-memory store. Keys expire after ttl_seconds.

    NOT suitable for multi-process deployments. Use RedisIdempotencyStore
    or a database-backed implementation for production clusters.
    """

    def __init__(self) -> None:
        self._store: dict[str, float] = {}  # key → expiry timestamp
        self._lock = threading.Lock()

    def exists(self, key: str) -> bool:
        with self._lock:
            expiry = self._store.get(key)
            if expiry is None:
                return False
            if time.monotonic() > expiry:
                del self._store[key]
                return False
            return True

    def mark(self, key: str, ttl_seconds: int = 86400) -> None:
        with self._lock:
            self._store[key] = time.monotonic() + ttl_seconds

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)


class IdempotencyChecker:
    """High-level wrapper — the thing you use in your handler."""

    def __init__(self, store: IdempotencyStore | None = None) -> None:
        self._store = store or InMemoryIdempotencyStore()

    def is_duplicate(self, key: str) -> bool:
        """Return True if this key has been seen before."""
        return self._store.exists(key)

    def mark_processed(self, key: str, ttl_seconds: int = 86400) -> None:
        """Record that this key has been processed."""
        self._store.mark(key, ttl_seconds)

    def check_and_mark(self, key: str, ttl_seconds: int = 86400) -> bool:
        """Atomically check + mark. Returns True if it IS a duplicate.

        Usage::
            if checker.check_and_mark(f"stk:{checkout_id}"):
                return {"status": "duplicate"}
            # safe to process
        """
        if self.is_duplicate(key):
            return True
        self.mark_processed(key, ttl_seconds)
        return False
