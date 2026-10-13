"""Worker policy metadata. Async runtime applies bounded local retries and admission."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple


@dataclass(frozen=True)
class RetryToDlqConfig:
    """Handler retry and dead-letter policy of one worker.

    Attributes:
        max_attempts: Handler attempts, including the first.
        dlq_topic: Topic receiving the dead letters.
        retry_count_field: Legacy metadata (accepted, not persisted).
        dlq_format: ``DeadLetterFormat`` building each dead letter; ``None`` keeps the
            2.0 diagnostic envelope.
        transient: Exception classes meaning "a dependency is down, nothing is wrong
            with the message": the job is retried with ``transient_backoff`` until it
            succeeds (or its partition is revoked); it is never dead-lettered and its
            offsets are not committed meanwhile.
        transient_backoff: ``Backoff`` of transient retries (``None``: exponential
            1, 2, 4 … 60 s with ±20 % jitter).
        backoff: ``Backoff`` between ordinary handler attempts (``None``: the runtime's
            full-jitter settings, as in 2.0).
    """

    max_attempts: int = 2
    dlq_topic: str = ""
    retry_count_field: str = "retry_count"
    dlq_format: Any = None
    transient: Tuple[type, ...] = ()
    transient_backoff: Any = None
    backoff: Any = None


@dataclass(frozen=True)
class CircuitBreakerConfig:
    failures: int = 5
    reset_sec: int = 30


@dataclass(frozen=True)
class RateLimitConfig:
    rps: float = 10.0
    burst: int = 10


def _iter_wrapped(fn: Callable[..., Any]):
    """Yield fn then walk __wrapped__ chain if present."""
    cur = fn
    seen = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        yield cur
        cur = getattr(cur, "__wrapped__", None)


def _try_update_registered_worker(fn: Callable[..., Any], key: str, value: Any) -> None:
    """
    If fn was already registered by @kafka_handler/@kafka_aggregator,
    update the WorkerSpec in the registry so decorator order doesn't matter.
    """
    try:
        # Local import to avoid hard circular import at module load time
        from framework.decorators import kafka_workers as kw  # type: ignore
    except Exception:
        return

    # If the decorator was applied after kafka_handler, the worker is already registered.
    # We tag functions with _qsint_worker_name at registration time.
    worker_name = getattr(fn, "_qsint_worker_name", None)

    if not worker_name:
        # Try to find on wrapped chain too
        for w in _iter_wrapped(fn):
            worker_name = getattr(w, "_qsint_worker_name", None)
            if worker_name:
                break

    if not worker_name:
        return

    try:
        kw.update_registered_worker_policy(str(worker_name), key, value)
    except Exception:
        # Registry may not be ready; ignore
        return


def _attach(fn: Callable[..., Any], key: str, value: Any) -> Callable[..., Any]:
    """
    Attach policy metadata to fn.

    Important: set attribute on fn AND its wrapped chain, and if fn is already
    registered as a worker, update registry in-place. This makes decorator order
    tolerant:
      - @rate_limit above @kafka_handler ✅
      - @rate_limit below @kafka_handler ✅
    """
    for w in _iter_wrapped(fn):
        try:
            setattr(w, key, value)
        except Exception:
            pass

    # Also set on the outer fn
    try:
        setattr(fn, key, value)
    except Exception:
        pass

    # If worker already registered, update registry spec too
    _try_update_registered_worker(fn, key, value)
    return fn


def retry_to_dlq(
    *,
    max_attempts: int = 2,
    dlq_topic: str,
    retry_count_field: str = "retry_count",
    dlq_format: Any = None,
    transient: Tuple[type, ...] = (),
    transient_backoff: Any = None,
    backoff: Any = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Attach a retry policy that escalates to a DLQ after N attempts.

    The 2.1 options (``dlq_format``, ``transient``, ``transient_backoff``,
    ``backoff``) are described on :class:`RetryToDlqConfig`.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    if not dlq_topic:
        raise ValueError("dlq_topic must be non-empty")
    transient = tuple(transient)
    if not all(isinstance(t, type) and issubclass(t, BaseException) for t in transient):
        raise ValueError("transient must contain exception classes")
    for name, value in (
        ("dlq_format", dlq_format),
        ("transient_backoff", transient_backoff),
        ("backoff", backoff),
    ):
        method = "build" if name == "dlq_format" else "delay"
        if value is not None and not callable(getattr(value, method, None)):
            raise ValueError(f"{name} must define {method}()")

    cfg = RetryToDlqConfig(
        max_attempts=int(max_attempts),
        dlq_topic=str(dlq_topic),
        retry_count_field=str(retry_count_field),
        dlq_format=dlq_format,
        transient=transient,
        transient_backoff=transient_backoff,
        backoff=backoff,
    )

    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        return _attach(fn, "_qsint_retry_to_dlq", cfg)

    return deco


def circuit_breaker(
    *,
    failures: int = 5,
    reset_sec: int = 30,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Attach a circuit breaker policy."""
    if failures < 1:
        raise ValueError("failures must be >= 1")
    if reset_sec < 1:
        raise ValueError("reset_sec must be >= 1")

    cfg = CircuitBreakerConfig(failures=int(failures), reset_sec=int(reset_sec))

    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        return _attach(fn, "_qsint_circuit_breaker", cfg)

    return deco


def rate_limit(
    *,
    rps: float,
    burst: Optional[int] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Attach a token-bucket rate limit policy."""
    if rps <= 0:
        raise ValueError("rps must be > 0")

    b = int(burst) if burst is not None else max(1, int(rps))
    if b < 1:
        raise ValueError("burst must be >= 1")

    cfg = RateLimitConfig(rps=float(rps), burst=b)

    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        return _attach(fn, "_qsint_rate_limit", cfg)

    return deco


def read_policy_metadata(fn: Callable[..., Any]) -> Dict[str, Any]:
    """Utility for runtimes: extract known policy metadata from function."""
    out: Dict[str, Any] = {}
    # Walk wrappers too
    for w in _iter_wrapped(fn):
        if "retry_to_dlq" not in out and hasattr(w, "_qsint_retry_to_dlq"):
            out["retry_to_dlq"] = getattr(w, "_qsint_retry_to_dlq")
        if "circuit_breaker" not in out and hasattr(w, "_qsint_circuit_breaker"):
            out["circuit_breaker"] = getattr(w, "_qsint_circuit_breaker")
        if "rate_limit" not in out and hasattr(w, "_qsint_rate_limit"):
            out["rate_limit"] = getattr(w, "_qsint_rate_limit")
    return out
