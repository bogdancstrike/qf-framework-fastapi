"""Kafka workers framework (registry + decorators)."""

from __future__ import annotations

import inspect
import uuid
import math
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, List, Optional, Literal

from .intern import RetryToDlqConfig, CircuitBreakerConfig, RateLimitConfig


WorkerKind = Literal["handler", "aggregator"]
OffsetReset = Literal["earliest", "latest"]
#: What happens to a record that is not JSON or not a JSON object (2.2.3).
Undecodable = Literal["dead_letter", "drop"]
UNDECODABLE_POLICIES = ("dead_letter", "drop")


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

    2.1 options (every default keeps the 2.0 behaviour):

    - ``consumer_group`` / ``bootstrap_servers`` / ``auto_offset_reset``: this worker's
      own consumer (group, cluster, start position). ``None`` uses the runtime's
      ``consumer_name`` / ``kafka_bootstrap_servers``. Workers sharing the three values
      share one consumer; outputs and dead letters go to the worker's own cluster.
    - ``split_arrays``: a JSON-array value carries one logical message per object element.
    - ``with_context``: the handler also receives ``context=`` (a ``RecordContext``, or a
      list aligned with the batch for bulk handlers).
    - ``inject_id``: ``False`` leaves messages without ``id`` untouched.
    - ``key_by``: dotted path of the output key (``None``: the source record key).
    - ``encoder``: bytes encoder of outputs (``None``: compact JSON).
    - ``raw``: the handler receives the record value bytes, undecoded.
    - ``enabled``: ``False`` keeps the worker declared but never started.

    2.2.3 option:

    - ``on_undecodable``: a record that is not JSON, or not a JSON object (with
      ``split_arrays``: an array without object elements), is dead-lettered at stage
      ``decode`` (``"dead_letter"``, the default) or dropped (``"drop"``: logged, counted
      in ``qf_decode_dropped_total{reason}`` and as a ``skipped`` message, its offset
      committed). An empty array is a skip either way.
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

    consumer_group: Optional[str] = None
    bootstrap_servers: Optional[str] = None
    auto_offset_reset: OffsetReset = "earliest"
    split_arrays: bool = False
    with_context: bool = False
    inject_id: bool = True
    key_by: Optional[str] = "id"
    encoder: Optional[Callable[[Any], bytes]] = None
    raw: bool = False
    enabled: bool = True
    on_undecodable: Undecodable = "dead_letter"


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
    if spec.auto_offset_reset not in ("earliest", "latest"):
        raise ValueError(f"Worker '{spec.name}': auto_offset_reset must be earliest or latest")
    for name in ("consumer_group", "bootstrap_servers"):
        value = getattr(spec, name)
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ValueError(f"Worker '{spec.name}': {name} must be a nonempty string or None")
    if spec.key_by is not None and (
        not isinstance(spec.key_by, str) or not all(spec.key_by.split("."))
    ):
        raise ValueError(f"Worker '{spec.name}': key_by must be a dotted path or None")
    if spec.encoder is not None and not callable(spec.encoder):
        raise ValueError(f"Worker '{spec.name}': encoder must be callable")
    if spec.raw and spec.split_arrays:
        raise ValueError(f"Worker '{spec.name}': raw and split_arrays are exclusive")
    if spec.on_undecodable not in UNDECODABLE_POLICIES:
        raise ValueError(
            f"Worker '{spec.name}': on_undecodable must be one of {UNDECODABLE_POLICIES}"
        )
    # An empty topics_out is a sink: the worker publishes nothing (returning outputs fails).

    if spec.kind == "handler" and spec.bulk_mode:
        if spec.batch_size < 2:
            raise ValueError(f"Worker '{spec.name}': batch_size must be >= 2 when bulk_mode=True")
        if spec.batch_timeout_ms < 1:
            raise ValueError(f"Worker '{spec.name}': batch_timeout_ms must be >= 1")

    if spec.kind == "aggregator":
        if spec.bulk_mode:
            raise ValueError("Aggregator workers cannot use bulk_mode")
        if spec.split_arrays or spec.raw:
            raise ValueError("Aggregator workers cannot use split_arrays or raw")
        if not spec.topics_out:
            raise ValueError(f"Aggregator '{spec.name}' must define topics_out")
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
        # A topic has one owner per consumer group (and cluster); other groups get their copy.
        scope = (spec.bootstrap_servers, spec.consumer_group)
        for topic in spec.topics_in:
            if (scope, topic) in topics:
                raise RuntimeError(f"Topic '{topic}' already handled by '{topics[scope, topic]}'")
            topics[scope, topic] = spec.name
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


#: ``@kafka_handler`` arguments that may be a callable ``(resolver) -> value``.
DEFERRABLE = (
    "topics_in",
    "topics_out",
    "consumer_group",
    "bootstrap_servers",
    "enabled",
    "max_workers",
    "batch_size",
    "batch_timeout_ms",
    "auto_offset_reset",
)
DECLARATION_ATTR = "_qsint_worker_declaration"


def _is_method(fn: Callable[..., Any]) -> bool:
    """A function whose first parameter is ``self`` or ``cls`` (declared in a class body)."""
    try:
        first = next(iter(inspect.signature(fn).parameters), None)
    except (TypeError, ValueError):
        return False
    return first in ("self", "cls")


def _build_spec(declaration: Dict[str, Any], fn: Callable[..., Any]) -> WorkerSpec:
    """A ``WorkerSpec`` from resolved declaration values and the (bound) function."""
    d = declaration
    return WorkerSpec(
        kind="aggregator" if d["aggregator"] or d["aggregate_by"] is not None else "handler",
        name=d["name"],
        topics_in=list(d["topics_in"]),
        topics_out=list(d["topics_out"]),
        metadatas=dict(d["metadatas"] or {}),
        max_workers=int(d["max_workers"]),
        retry_to_dlq=_get_attr_anywhere(fn, "_qsint_retry_to_dlq"),
        circuit_breaker=_get_attr_anywhere(fn, "_qsint_circuit_breaker"),
        rate_limit=_get_attr_anywhere(fn, "_qsint_rate_limit"),
        bulk_mode=bool(d["bulk_mode"]),
        batch_size=int(d["batch_size"]),
        batch_timeout_ms=int(d["batch_timeout_ms"]),
        aggregate_by=d["aggregate_by"]
        if d["aggregate_by"] is not None
        else ("id" if d["aggregator"] else None),
        aggregator_timeout_sec=int(d["aggregator_timeout_sec"]),
        fn=fn,
        consumer_group=d["consumer_group"],
        bootstrap_servers=d["bootstrap_servers"],
        auto_offset_reset=d["auto_offset_reset"],
        split_arrays=bool(d["split_arrays"]),
        with_context=bool(d["with_context"]),
        inject_id=bool(d["inject_id"]),
        key_by=d["key_by"],
        encoder=d["encoder"],
        raw=bool(d["raw"]),
        enabled=bool(d["enabled"]),
        on_undecodable=d["on_undecodable"],
    )


def kafka_handler(
    *,
    name: str,
    topics_in: Any,
    topics_out: Any,
    metadatas: Optional[Dict[str, Any]] = None,
    max_workers: Any = 4,
    bulk_mode: bool = False,
    batch_size: Any = 50,
    batch_timeout_ms: Any = 200,
    aggregator: bool = False,
    aggregate_by: Optional[str] = None,
    aggregator_timeout_sec: int = 600,
    consumer_group: Any = None,
    bootstrap_servers: Any = None,
    auto_offset_reset: Any = "earliest",
    split_arrays: bool = False,
    with_context: bool = False,
    inject_id: bool = True,
    key_by: Optional[str] = "id",
    encoder: Optional[Callable[[Any], bytes]] = None,
    raw: bool = False,
    enabled: Any = True,
    on_undecodable: Undecodable = "dead_letter",
):
    """Declare a worker: a normal handler or a correlation join.

    `aggregator=True` defaults the join key to `id`. Supplying `aggregate_by`
    also selects aggregation; dotted paths are supported. The 2.1 keyword
    options (`consumer_group` … `enabled`) are described on :class:`WorkerSpec`;
    their defaults keep the 2.0 behaviour; `on_undecodable` (2.2.3) likewise. `topics_out=[]`
    declares a sink.

    A module-level function is registered at import (2.0 behaviour). A method
    (first parameter ``self``/``cls``), or a function with a deferred value, is
    only *declared*: :func:`collect_workers` builds its ``WorkerSpec`` bound to an
    instance (or module), resolving every deferred value ``(resolver) -> value``
    (allowed for: ``topics_in``, ``topics_out``, ``consumer_group``,
    ``bootstrap_servers``, ``enabled``, ``max_workers``, ``batch_size``,
    ``batch_timeout_ms``, ``auto_offset_reset``).
    """
    if isinstance(topics_in, str) or isinstance(topics_out, str):
        raise ValueError("topics_in and topics_out must be sequences of topic names")
    declaration = {
        "name": name,
        "topics_in": topics_in,
        "topics_out": topics_out,
        "metadatas": metadatas,
        "max_workers": max_workers,
        "bulk_mode": bulk_mode,
        "batch_size": batch_size,
        "batch_timeout_ms": batch_timeout_ms,
        "aggregator": aggregator,
        "aggregate_by": aggregate_by,
        "aggregator_timeout_sec": aggregator_timeout_sec,
        "consumer_group": consumer_group,
        "bootstrap_servers": bootstrap_servers,
        "auto_offset_reset": auto_offset_reset,
        "split_arrays": split_arrays,
        "with_context": with_context,
        "inject_id": inject_id,
        "key_by": key_by,
        "encoder": encoder,
        "raw": raw,
        "enabled": enabled,
        "on_undecodable": on_undecodable,
    }
    deferred = any(callable(declaration[key]) for key in DEFERRABLE)

    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        if deferred or _is_method(fn):
            setattr(fn, DECLARATION_ATTR, declaration)
            return fn
        register_worker(_build_spec(declaration, fn))

        # Tag fn so policy decorators applied later can update registry
        try:
            setattr(fn, "_qsint_worker_name", name)
        except Exception:
            pass

        return fn

    return deco


def _declared_function(raw: Any) -> Optional[Callable[..., Any]]:
    """The function carrying a worker declaration behind ``raw`` (unwrapping descriptors)."""
    if isinstance(raw, (staticmethod, classmethod)):
        raw = raw.__func__
    if not callable(raw):
        return None
    for candidate in _iter_wrapped(raw):
        if hasattr(candidate, DECLARATION_ATTR):
            return candidate
    return None


def collect_workers(
    owner: Any,
    resolver: Any = None,
    *,
    override: Optional[Callable[[WorkerSpec], WorkerSpec]] = None,
) -> List[WorkerSpec]:
    """``WorkerSpec``s of every ``@kafka_handler`` declared on ``owner``.

    Args:
        owner: An instance (its class's decorated methods are bound to it) or a module.
        resolver: Passed to every deferred value ``(resolver) -> value``.
        override: Optional final hook applied to each spec (e.g. per-environment
            overrides) before validation.

    Returns:
        Validated specs in declaration-name order, disabled ones included
        (``enabled=False``); runtimes skip those and report them as disabled.

    Raises:
        ValueError, RuntimeError: an invalid spec, a duplicate name, or a topic owned
            twice in the same consumer group.
    """
    specs = []
    for attribute in sorted(dir(owner)):
        if attribute.startswith("__"):
            continue
        try:
            raw = inspect.getattr_static(owner, attribute)
        except AttributeError:
            continue
        declared = _declared_function(raw)
        if declared is None:
            continue
        values = dict(getattr(declared, DECLARATION_ATTR))
        for key in DEFERRABLE:
            if callable(values[key]):
                values[key] = values[key](resolver)
        bound = getattr(owner, attribute)
        policies_from = raw.__func__ if isinstance(raw, (staticmethod, classmethod)) else raw
        spec = _build_spec(values, bound)
        spec = replace(
            spec,
            retry_to_dlq=_get_attr_anywhere(policies_from, "_qsint_retry_to_dlq"),
            circuit_breaker=_get_attr_anywhere(policies_from, "_qsint_circuit_breaker"),
            rate_limit=_get_attr_anywhere(policies_from, "_qsint_rate_limit"),
        )
        if override is not None:
            spec = override(spec)
        specs.append(spec)
    return validate_workers(specs)


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
