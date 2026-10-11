"""Kafka workers framework (registry + decorators)."""

from __future__ import annotations

import uuid
import math
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, List, Optional, Literal

from .intern import RetryToDlqConfig, CircuitBreakerConfig, RateLimitConfig


WorkerKind = Literal["handler", "aggregator"]


def _get_by_path(obj: dict, path: str) -> Any:
    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _iter_wrapped(fn: Callable[..., Any]):
    cur = fn
    seen = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        yield cur
        cur = getattr(cur, "__wrapped__", None)


def _get_attr_anywhere(fn: Callable[..., Any], attr: str):
    for w in _iter_wrapped(fn):
        if hasattr(w, attr):
            return getattr(w, attr)
    return getattr(fn, attr, None)


def ensure_message_id(message: dict) -> str:
    mid = message.get("id")
    if mid is None or mid == "":
        mid = str(uuid.uuid4())
        message["id"] = mid
    return str(mid)


@dataclass(frozen=True)
class WorkerSpec:
    """Worker configuration, snapshotted and validated before runtime startup.

    The function receives (message_or_batch, consumer_name, metadatas). Policy
    decorators replace this frozen record rather than wrapping runtime retries.
    Mutable topic lists and metadata are copied at the runtime boundary.
    """

    kind: WorkerKind
    name: str
    topics_in: List[str]
    topics_out: List[str]
    metadatas: Dict[str, Any]
    max_workers: int

    retry_to_dlq: Optional[RetryToDlqConfig] = None
    circuit_breaker: Optional[CircuitBreakerConfig] = None
    rate_limit: Optional[RateLimitConfig] = None

    bulk_mode: bool = False
    batch_size: int = 1
    batch_timeout_ms: int = 200

    aggregate_by: Optional[str] = None
    aggregator_timeout_sec: int = 600

    fn: Callable[..., Any] = lambda *a, **k: None


_WORKERS_BY_NAME: Dict[str, WorkerSpec] = {}
_TOPIC_TO_WORKER: Dict[str, str] = {}


def update_registered_worker_policy(worker_name: str, key: str, value: Any) -> None:
    """
    Called by policy decorators if applied after @kafka_handler.
    WorkerSpec is frozen; we replace it in registry.
    """
    spec = _WORKERS_BY_NAME.get(worker_name)
    if not spec:
        return

    if key == "_qsint_retry_to_dlq" and isinstance(value, RetryToDlqConfig):
        _WORKERS_BY_NAME[worker_name] = replace(spec, retry_to_dlq=value)
    elif key == "_qsint_circuit_breaker" and isinstance(value, CircuitBreakerConfig):
        _WORKERS_BY_NAME[worker_name] = replace(spec, circuit_breaker=value)
    elif key == "_qsint_rate_limit" and isinstance(value, RateLimitConfig):
        _WORKERS_BY_NAME[worker_name] = replace(spec, rate_limit=value)


def validate_worker(spec: WorkerSpec) -> None:
    """Validate one specification without mutating the global decorator registry."""
    if spec.kind not in {"handler", "aggregator"}:
        raise ValueError(f"Unknown worker kind: {spec.kind}")
    if not callable(spec.fn):
        raise ValueError("Worker fn must be callable")
    for name in ("max_workers", "batch_size", "batch_timeout_ms", "aggregator_timeout_sec"):
        value = getattr(spec, name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive integer")
    if spec.circuit_breaker and (
        spec.circuit_breaker.failures < 1
        or spec.circuit_breaker.reset_sec < 1
        or not math.isfinite(spec.circuit_breaker.reset_sec)
    ):
        raise ValueError("Invalid circuit breaker policy")
    if spec.rate_limit and (
        not math.isfinite(spec.rate_limit.rps)
        or spec.rate_limit.rps <= 0
        or spec.rate_limit.burst < 1
    ):
        raise ValueError("Invalid rate limit policy")
    if spec.retry_to_dlq and (
        spec.retry_to_dlq.max_attempts < 1 or not spec.retry_to_dlq.dlq_topic
    ):
        raise ValueError("Invalid retry-to-DLQ policy")
    if spec.max_workers < 1:
        raise ValueError("max_workers must be >= 1")
    if len(set(spec.topics_in)) != len(spec.topics_in):
        raise ValueError("Input topics must be unique")
    if len(set(spec.topics_out)) != len(spec.topics_out):
        raise ValueError("Output topics must be unique")
    if (
        not isinstance(spec.name, str)
        or not spec.name.strip()
        or isinstance(spec.topics_in, str)
        or isinstance(spec.topics_out, str)
        or any(not isinstance(t, str) or not t.strip() for t in [*spec.topics_in, *spec.topics_out])
    ):
        raise ValueError("Worker name and topics must be nonempty")
    if spec.aggregator_timeout_sec < 1:
        raise ValueError("aggregator_timeout_sec must be >= 1")
    if not spec.topics_in:
        raise ValueError(f"Worker '{spec.name}' must define topics_in")
    if not spec.topics_out:
        raise ValueError(f"Worker '{spec.name}' must define topics_out")

    if spec.kind == "handler" and spec.bulk_mode:
        if spec.batch_size < 2:
            raise ValueError(f"Worker '{spec.name}': batch_size must be >= 2 when bulk_mode=True")
        if spec.batch_timeout_ms < 1:
            raise ValueError(f"Worker '{spec.name}': batch_timeout_ms must be >= 1")

    if spec.kind == "aggregator":
        if spec.bulk_mode:
            raise ValueError("Aggregator workers cannot use bulk_mode")
        if not isinstance(spec.aggregate_by, str) or not all(spec.aggregate_by.split(".")):
            raise ValueError(f"Aggregator '{spec.name}' must define aggregate_by (e.g. 'id')")
        if len(spec.topics_in) < 2:
            raise ValueError(f"Aggregator '{spec.name}' must define 2+ topics_in")


def validate_workers(workers) -> list[WorkerSpec]:
    """Validate and snapshot a complete worker set, including name/topic collisions.

    Both decorator registration and explicit runtime injection use this boundary.
    It is deliberately independent of global state so applications can compose
    isolated runtimes and test them without importing a registry singleton.
    """
    names, topics, result = set(), {}, []
    for spec in workers:
        validate_worker(spec)
        if spec.name in names:
            raise RuntimeError(f"Duplicate worker name '{spec.name}'")
        names.add(spec.name)
        for topic in spec.topics_in:
            if topic in topics:
                raise RuntimeError(f"Topic '{topic}' already handled by '{topics[topic]}'")
            topics[topic] = spec.name
        result.append(
            replace(
                spec,
                topics_in=list(spec.topics_in),
                topics_out=list(spec.topics_out),
                metadatas=deepcopy(spec.metadatas),
            )
        )
    return result


def register_worker(spec: WorkerSpec) -> None:
    validate_worker(spec)
    if spec.name in _WORKERS_BY_NAME:
        raise RuntimeError(f"Duplicate worker name '{spec.name}'")
    for topic in spec.topics_in:
        if topic in _TOPIC_TO_WORKER:
            raise RuntimeError(f"Topic '{topic}' already handled by '{_TOPIC_TO_WORKER[topic]}'")
    spec = replace(
        spec,
        topics_in=list(spec.topics_in),
        topics_out=list(spec.topics_out),
        metadatas=deepcopy(spec.metadatas),
    )
    _WORKERS_BY_NAME[spec.name] = spec
    for t in spec.topics_in:
        _TOPIC_TO_WORKER[t] = spec.name


def all_topics() -> List[str]:
    return sorted(_TOPIC_TO_WORKER.keys())


def all_workers() -> List[WorkerSpec]:
    return list(_WORKERS_BY_NAME.values())


def worker_for_topic(topic: str) -> Optional[WorkerSpec]:
    name = _TOPIC_TO_WORKER.get(topic)
    return _WORKERS_BY_NAME.get(name) if name else None


def compute_aggregate_key(spec: WorkerSpec, message: dict) -> str:
    if spec.aggregate_by:
        v = _get_by_path(message, spec.aggregate_by)
        if (
            isinstance(v, (str, int, float, bool))
            and v != ""
            and not (isinstance(v, float) and not math.isfinite(v))
        ):
            return str(v)
    raise ValueError(f"Missing or invalid aggregation key: {spec.aggregate_by}")


def kafka_handler(
    *,
    name: str,
    topics_in: List[str],
    topics_out: List[str],
    metadatas: Optional[Dict[str, Any]] = None,
    max_workers: int = 4,
    bulk_mode: bool = False,
    batch_size: int = 50,
    batch_timeout_ms: int = 200,
    aggregator: bool = False,
    aggregate_by: Optional[str] = None,
    aggregator_timeout_sec: int = 600,
):
    """Register a normal worker or a correlation join.

    `aggregator=True` defaults the join key to `id`. Supplying `aggregate_by`
    also selects aggregation; dotted paths are supported.
    """
    if isinstance(topics_in, str) or isinstance(topics_out, str):
        raise ValueError("topics_in and topics_out must be sequences of topic names")

    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        # Read policies from anywhere in wrapper chain
        retry_cfg = _get_attr_anywhere(fn, "_qsint_retry_to_dlq")
        cb_cfg = _get_attr_anywhere(fn, "_qsint_circuit_breaker")
        rl_cfg = _get_attr_anywhere(fn, "_qsint_rate_limit")

        register_worker(
            WorkerSpec(
                kind="aggregator" if aggregator or aggregate_by is not None else "handler",
                name=name,
                topics_in=list(topics_in),
                topics_out=list(topics_out),
                metadatas=dict(metadatas or {}),
                max_workers=int(max_workers),
                retry_to_dlq=retry_cfg,
                circuit_breaker=cb_cfg,
                rate_limit=rl_cfg,
                bulk_mode=bool(bulk_mode),
                batch_size=int(batch_size),
                batch_timeout_ms=int(batch_timeout_ms),
                aggregate_by=aggregate_by
                if aggregate_by is not None
                else ("id" if aggregator else None),
                aggregator_timeout_sec=int(aggregator_timeout_sec),
                fn=fn,
            )
        )

        # Tag fn so policy decorators applied later can update registry
        try:
            setattr(fn, "_qsint_worker_name", name)
        except Exception:
            pass

        return fn

    return deco


def kafka_aggregator(
    *,
    name: str,
    topics_in: List[str],
    topics_out: List[str],
    aggregate_by: str = "id",
    aggregator_timeout_sec: int = 600,
    metadatas: Optional[Dict[str, Any]] = None,
    max_workers: int = 4,
):
    """Compatibility spelling for `kafka_handler(aggregator=True, ...)`."""
    return kafka_handler(
        name=name,
        topics_in=topics_in,
        topics_out=topics_out,
        aggregator=True,
        aggregate_by=aggregate_by,
        aggregator_timeout_sec=aggregator_timeout_sec,
        metadatas=metadatas,
        max_workers=max_workers,
    )
