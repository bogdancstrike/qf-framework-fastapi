"""`VersionedJsonCache`: a Redis read-through JSON cache keyed by a name and a data version.

The pattern: data derived from a configuration (a database of settings, a catalogue) is
cached under ``(name, version)``. Whoever changes the configuration bumps the version, so
every replica sees the change on its next read, with no TTL to wait for and no invalidation
to miss. The caller reads the current version (cheaply) on every request.

The cache is an optimisation only and never fails a request:

- every Redis call has a time budget (`budget_s`);
- failures and timeouts are counted by a `CircuitBreaker`; while it is open Redis is not
  called at all;
- a failure, timeout, open breaker or undecodable value degrades to a rebuild from the
  loader, logged as ``cache_unavailable`` / ``cache_corrupt``;
- version ``0`` or lower ("unknown") is never cached: it could never be invalidated.

With ``memo=True`` the cache also keeps, per name, the last value it returned with its
version and returns it while the version is unchanged: no Redis round trip and no JSON
decode on the hot path. Only use it for values nobody mutates.

Example::

    cache = VersionedJsonCache(redis, key=lambda name, version: f"myapp:{name}:{version}")
    fields = await cache.get_or_build("fields", version, ttl_s=3600, loader=load_fields)
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast

from ..commons.events import EventLogger, get_event_logger
from ..resilience import CircuitBreaker

T = TypeVar("T")

#: ``(name, version) -> Redis key``.
KeyFunction = Callable[[str, int], str]
#: ``(operation, outcome, started_perf_counter)``: called once per Redis call (metrics).
CallObserver = Callable[[str, str, float], None]

DEFAULT_BUDGET_S = 0.5


def _same(value: Any) -> Any:
    return value


def _ignore(operation: str, outcome: str, started: float) -> None:
    """Default `CallObserver`: records nothing."""


class VersionedJsonCache:
    """Decorator over a loader (VersionedJsonCache): read-through JSON cache by name and version.

    Args:
        client: A ``redis.asyncio`` client (``decode_responses`` either way).
        key: Builds the Redis key of a ``(name, version)`` pair.
        breaker: Shared by every cache of the process that talks to the same Redis.
        budget_s: Time budget of one Redis call.
        memo: Keep the last value per name in process while its version is current.
        observe: Called after every Redis call, for metrics.
        log: Where ``cache_unavailable`` / ``cache_corrupt`` are logged.
    """

    def __init__(
        self,
        client: Any,
        *,
        key: KeyFunction,
        breaker: CircuitBreaker | None = None,
        budget_s: float = DEFAULT_BUDGET_S,
        memo: bool = False,
        observe: CallObserver = _ignore,
        log: EventLogger | None = None,
    ) -> None:
        self._client = client
        self._key = key
        self._breaker = breaker or CircuitBreaker(3, 30.0)
        self._budget = budget_s
        self._memo: dict[str, tuple[int, Any]] | None = {} if memo else None
        self._observe = observe
        self._log = log or get_event_logger("framework.redis.cache")

    def key(self, name: str, version: int) -> str:
        """The Redis key of `name` at `version`."""
        return self._key(name, version)

    async def get_or_build(
        self,
        name: str,
        version: int,
        ttl_s: int,
        loader: Callable[[], Awaitable[T]],
        *,
        encode: Callable[[T], Any] = _same,
        decode: Callable[[Any], T] = _same,
    ) -> T:
        """The cached value of `name` at `version`, or a fresh build (cached best effort).

        `encode` turns a built value into JSON-able data and `decode` reverses it; a `decode`
        failure (`KeyError`, `TypeError`, `ValueError`) is treated as a miss.
        """
        if version <= 0:
            return await loader()
        memoised = self._memoised(name, version)
        if memoised is not None:
            return cast(T, memoised[1])
        key = self.key(name, version)
        cached = await self._get(key)
        if cached is not None:
            try:
                return self._remember(name, version, decode(cached))
            except (KeyError, TypeError, ValueError):
                self._log.warning("cache_corrupt", key=key)
        value = await loader()
        await self._set(key, encode(value), ttl_s)
        return self._remember(name, version, value)

    def _memoised(self, name: str, version: int) -> tuple[int, Any] | None:
        if self._memo is None:
            return None
        entry = self._memo.get(name)
        return entry if entry is not None and entry[0] == version else None

    def _remember(self, name: str, version: int, value: T) -> T:
        if self._memo is not None:
            self._memo[name] = (version, value)
        return value

    async def _get(self, key: str) -> Any:
        if not self._breaker.allow():
            return None
        started = time.perf_counter()
        try:
            raw = await asyncio.wait_for(self._client.get(key), self._budget)
        except Exception as exc:
            self._failed("get", key, exc, started)
            return None
        self._breaker.success()
        self._observe("get", "hit" if raw is not None else "miss", started)
        return _decode_json(raw)

    async def _set(self, key: str, value: Any, ttl_s: int) -> None:
        if not self._breaker.allow():
            return
        started = time.perf_counter()
        try:
            payload = json.dumps(value, default=str)
            await asyncio.wait_for(self._client.set(key, payload, ex=ttl_s), self._budget)
        except Exception as exc:
            self._failed("set", key, exc, started)
            return
        self._breaker.success()
        self._observe("set", "ok", started)

    def _failed(self, operation: str, key: str, exc: Exception, started: float) -> None:
        self._breaker.failure()
        self._observe(operation, "error", started)
        self._log.warning("cache_unavailable", key=key, error=type(exc).__name__)


def _decode_json(raw: Any) -> Any:
    """The JSON value of a Redis reply; `None` for a miss or an undecodable value."""
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None
