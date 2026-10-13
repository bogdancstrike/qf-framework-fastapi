"""Start-up dependency gate: an application serves nothing until its third parties answer.

Call `wait_for_dependencies` from the application lifespan *before* yielding. Until it
returns, the ASGI lifespan has not finished starting, so the server accepts no connection:
``/health/live`` and ``/health/ready`` are unreachable (neither live nor ready) and no worker
runs. Each check runs with its own timeout; failing ones are retried together with an
exponential backoff and logged (``dependency_unavailable`` with the error class and a short
message, never credentials). When all answer, ``dependencies_ready`` is logged. Past the
deadline `DependenciesUnavailable` is raised, start-up fails and the orchestrator restarts
the process.

Example::

    async def postgres() -> None:
        await engine.execute("SELECT 1")          # raise when unreachable

    await wait_for_dependencies({"postgres": postgres}, required=("postgres",), timeout_s=300)
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass

from ..commons.events import EventLogger, get_event_logger

#: A dependency check: returns when the dependency answers, raises otherwise.
Check = Callable[[], Awaitable[None]]


class DependenciesUnavailable(RuntimeError):
    """A required dependency did not answer within the start-up deadline."""


@dataclass(frozen=True, slots=True)
class GatePolicy:
    """Timing of the gate.

    Attributes:
        check_timeout_s: Time budget of one check.
        backoff_start_s: First wait between two rounds.
        backoff_cap_s: Longest wait between two rounds.
    """

    check_timeout_s: float = 5.0
    backoff_start_s: float = 1.0
    backoff_cap_s: float = 15.0


async def _attempt(check: Check, timeout_s: float) -> str | None:
    """`None` when the check passed, else a short error description."""
    try:
        await asyncio.wait_for(check(), timeout=timeout_s)
        return None
    except TimeoutError:
        return f"no answer within {timeout_s:.0f}s"
    except Exception as exc:
        return f"{type(exc).__name__}: {str(exc)[:200]}"


class _Gate:
    """One run of the gate: the pending checks, the attempt count and the backoff."""

    def __init__(
        self, checks: Mapping[str, Check], pending: list[str], policy: GatePolicy, log: EventLogger
    ) -> None:
        self.checks, self.pending, self.policy, self.log = checks, pending, policy, log
        self.attempt = 0
        self.delay = policy.backoff_start_s

    async def round(self) -> dict[str, str]:
        """Run every pending check once; returns the failing ones with their errors."""
        self.attempt += 1
        timeout = self.policy.check_timeout_s
        errors = await asyncio.gather(*(_attempt(self.checks[n], timeout) for n in self.pending))
        failing = {n: e for n, e in zip(self.pending, errors, strict=True) if e}
        for name in self.pending:
            if name not in failing:
                self.log.info("dependency_available", dependency=name, attempt=self.attempt)
        for name, error in failing.items():
            self.log.warning(
                "dependency_unavailable", dependency=name, attempt=self.attempt, error=error
            )
        self.pending = list(failing)
        return failing

    def next_delay(self) -> float:
        """The wait before the next round (doubles up to the cap)."""
        delay = self.delay
        self.delay = min(self.delay * 2, self.policy.backoff_cap_s)
        return delay


async def wait_for_dependencies(
    checks: Mapping[str, Check],
    *,
    required: Sequence[str],
    timeout_s: float,
    policy: GatePolicy = GatePolicy(),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    log: EventLogger | None = None,
) -> None:
    """Block until every `required` check passes.

    Args:
        checks: Every check the application knows, by dependency name.
        required: The dependencies to wait for; names without a check are logged and skipped.
        timeout_s: Overall deadline in seconds; ``0`` waits forever.
        policy: Check timeout and backoff.
        sleep: Injected by tests.
        log: Where the gate's events are logged (default ``framework.health``).

    Raises:
        DependenciesUnavailable: a required dependency did not answer before the deadline.
    """
    log = log or get_event_logger("framework.health")
    unknown = [name for name in required if name not in checks]
    if unknown:
        log.info("dependency_check_not_applicable", dependencies=unknown)
    gate = _Gate(checks, [name for name in required if name in checks], policy, log)
    started = time.monotonic()
    while gate.pending:
        failing = await gate.round()
        if not failing:
            break
        elapsed = time.monotonic() - started
        if timeout_s and elapsed + gate.delay > timeout_s:
            raise DependenciesUnavailable(
                f"dependencies unavailable after {elapsed:.0f}s: {', '.join(sorted(failing))}"
            )
        await sleep(gate.next_delay())
    log.info(
        "dependencies_ready",
        dependencies=[name for name in required if name in checks],
        duration_ms=round((time.monotonic() - started) * 1000),
    )
