"""Event-loop owned admission policies with monotonic clocks."""

import time


class Admission:
    def __init__(self, spec):
        self.spec = spec
        self.tokens = float(spec.rate_limit.burst) if spec.rate_limit else 0
        self.updated = time.monotonic()
        self.failures = 0
        self.open_until = 0.0
        self.probe = False

    def allow(self):
        now = time.monotonic()
        cb = self.spec.circuit_breaker
        if cb and (self.open_until > now or self.probe):
            return False
        rl = self.spec.rate_limit
        if rl:
            self.tokens = min(rl.burst, self.tokens + (now - self.updated) * rl.rps)
            self.updated = now
            if self.tokens < 1:
                return False
            self.tokens -= 1
        if cb and self.open_until:
            self.probe = True
        return True

    def success(self):
        self.failures, self.open_until, self.probe = 0, 0.0, False

    def failure(self):
        cb = self.spec.circuit_breaker
        if not cb:
            return
        self.failures += 1
        if self.probe or self.failures >= cb.failures:
            self.open_until = time.monotonic() + cb.reset_sec
            self.failures = 0
        self.probe = False
