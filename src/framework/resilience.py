"""`CircuitBreaker`: an explicit breaker for callers that decide success and failure themselves.

`framework.decorators.call_circuit_breaker` wraps a function and raises `CircuitOpenError`
while open. `CircuitBreaker` is the state machine on its own, for code that must *degrade*
rather than raise when a dependency is down: a cache that serves a rebuild, a client that
answers "unavailable". One breaker guards one dependency and lives as long as the process
(create it in a lifespan or composition root, never at import).

Usage::

    breaker = CircuitBreaker(threshold=3, open_s=30.0)
    if breaker.allow():
        try:
            value = await redis.get(key)
        except RedisError:
            breaker.failure()
        else:
            breaker.success()
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Literal

BreakerState = Literal["closed", "open", "half_open"]


class CircuitBreaker:
    """State (CircuitBreaker): opens after `threshold` consecutive failures for `open_s` seconds,
    then lets exactly one probe call through (half-open).

    A failed probe re-opens the circuit for another `open_s`; a successful one closes it.
    Not thread-safe: use one breaker per event loop.

    Args:
        threshold: Consecutive failures that open the circuit.
        open_s: Seconds the circuit stays open before the probe.
        clock: Monotonic clock (injected by tests).
    """

    def __init__(
        self,
        threshold: int = 5,
        open_s: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if threshold < 1 or open_s <= 0:
            raise ValueError("threshold must be >= 1 and open_s > 0")
        self.threshold = threshold
        self.open_s = open_s
        self._clock = clock
        self._failures = 0
        self._opened_at: float | None = None
        self._probing = False

    @property
    def state(self) -> BreakerState:
        """``closed``, ``open``, or ``half_open`` once `open_s` has elapsed."""
        if self._opened_at is None:
            return "closed"
        if self._clock() - self._opened_at >= self.open_s:
            return "half_open"
        return "open"

    def allow(self) -> bool:
        """`True` when a call may be attempted (closed, or the single half-open probe)."""
        state = self.state
        if state == "closed":
            return True
        if state == "half_open" and not self._probing:
            self._probing = True
            return True
        return False

    def success(self) -> None:
        """Record a successful call: the circuit closes and the failure count resets."""
        self._failures = 0
        self._opened_at = None
        self._probing = False

    def failure(self) -> None:
        """Record a failed call: opens the circuit at the threshold or after a failed probe."""
        self._probing = False
        self._failures += 1
        if self._opened_at is not None or self._failures >= self.threshold:
            self._opened_at = self._clock()

    def reset(self) -> None:
        """Back to closed with no failures (tests and operator tooling)."""
        self.success()
