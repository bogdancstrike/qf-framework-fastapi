"""Event-loop owned admission policies with monotonic clocks."""

import time


class Admission:
    """Per-worker token bucket and generation-fenced circuit, owned by one loop.

    Every dispatched job retains its admission generation. Opening a circuit or
    starting a half-open probe invalidates older completions. A cancelled probe
    releases its slot without reporting a business failure.
    """

    def __init__(self, spec):
        self.spec = spec
        self.tokens = float(spec.rate_limit.burst) if spec.rate_limit else 0
        self.updated = time.monotonic()
        self.failures = 0
        self.open_until = 0.0
        self.probe = False
        self.generation = 0

    def acquire(self) -> int | None:
        """Consume admission and return a completion ticket, or None if blocked."""
        now = time.monotonic()
        cb = self.spec.circuit_breaker
        if cb and (self.open_until > now or self.probe):
            return None
        rl = self.spec.rate_limit
        if rl:
            self.tokens = min(rl.burst, self.tokens + (now - self.updated) * rl.rps)
            self.updated = now
            if self.tokens < 1:
                return None
            self.tokens -= 1
        if cb and self.open_until:
            self.probe = True
            self.generation += 1
        return self.generation

    def success(self, generation: int):
        if generation != self.generation:
            return
        self.failures, self.open_until, self.probe = 0, 0.0, False

    def failure(self, generation: int):
        if generation != self.generation:
            return
        cb = self.spec.circuit_breaker
        if not cb:
            return
        self.failures += 1
        if self.probe or self.failures >= cb.failures:
            self.open_until = time.monotonic() + cb.reset_sec
            self.failures = 0
            self.generation += 1
        self.probe = False

    def abandon(self, generation: int):
        """Release a cancelled/revoked probe; retain the existing open state."""
        if generation == self.generation and self.probe:
            self.probe = False
            self.generation += 1
