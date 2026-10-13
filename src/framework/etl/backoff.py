"""Delay strategies between handler attempts (Strategy).

``FullJitter`` is the 2.0 behaviour (a uniform delay in ``[0, min(cap, base·2^n)]``)
and stays the default. ``ExponentialBackoff`` gives the predictable schedule
``base·factor^n`` capped and scaled by ``±jitter``, which operators expect for
waits on a transient dependency outage.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Protocol


class Backoff(Protocol):
    """Strategy: seconds to wait before attempt ``attempt + 1`` (``attempt`` counts from 0)."""

    def delay(self, attempt: int) -> float:
        """The wait after failed attempt ``attempt``."""
        ...


@dataclass(frozen=True)
class FullJitter:
    """Uniform delay in ``[0, min(cap_s, base_s · 2^attempt)]`` (AWS "full jitter")."""

    base_s: float = 0.25
    cap_s: float = 10.0

    def delay(self, attempt: int) -> float:
        """A random delay below the exponential bound."""
        return random.uniform(0, min(self.cap_s, self.base_s * 2 ** min(attempt, 30)))


@dataclass(frozen=True)
class ExponentialBackoff:
    """``base_s · factor^attempt`` capped at ``cap_s``, times a uniform factor in ``1 ± jitter``.

    With the defaults: about 1, 2, 4, 8, 16, 32, 60, 60 … seconds.
    """

    base_s: float = 1.0
    factor: float = 2.0
    cap_s: float = 60.0
    jitter: float = 0.2

    def __post_init__(self) -> None:
        if self.base_s < 0 or self.factor < 1 or self.cap_s <= 0 or not 0 <= self.jitter < 1:
            raise ValueError("Invalid exponential backoff")

    def delay(self, attempt: int) -> float:
        """The capped exponential delay with jitter."""
        bound = min(self.cap_s, self.base_s * self.factor ** min(attempt, 64))
        return bound * random.uniform(1 - self.jitter, 1 + self.jitter)
