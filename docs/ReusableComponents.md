# Reusable application components (2.2)

Building blocks that QSINT services had written each for themselves. Each is an opt-in import.
The legacy HTTP contract has its own page: [Operations-HTTP.md](Operations-HTTP.md).

## Start-up dependency gate — `framework.health`

An application should be neither live nor ready, and run no worker, until the third parties it
needs answer. Call the gate from the lifespan before yielding: until the lifespan finishes
starting, the server accepts no connection.

```python
from framework.health import GatePolicy, wait_for_dependencies


async def postgres() -> None:
    if not await db.ping():
        raise ConnectionError("SELECT 1 failed")


@asynccontextmanager
async def lifespan(app):
    await wait_for_dependencies(
        {"postgres": postgres, "kafka": kafka_metadata},
        required=settings.startup_dependencies,      # names without a check are logged and skipped
        timeout_s=300,                               # 0 = wait forever
        policy=GatePolicy(check_timeout_s=5, backoff_start_s=1, backoff_cap_s=15),
        log=my_event_logger,                         # default: framework.health
    )
    yield
```

Checks run concurrently, each with its own timeout; failing ones are retried together with
exponential backoff. Events: `dependency_available`, `dependency_unavailable` (error class and
a short message, never credentials), `dependency_check_not_applicable`, `dependencies_ready`.
Past the deadline `DependenciesUnavailable` is raised: start-up fails and the orchestrator
restarts the process.

## Detailed readiness — `framework.health`

```python
from framework.health import install_readiness_routes, readiness_status, run_probes


async def report(app):
    results = await run_probes({"postgres": db.ping(), "redis": redis.ping()}, timeout_s=3)
    checks = {name: {"ok": ok} for name, ok in results.items()}
    status = readiness_status(checks, required={"postgres"}, started=app.state.started)
    return {"status": status, "checks": checks}


install_readiness_routes(app, report)     # /health/ready and /health/dependencies
```

`not_ready` (503) when the application has not started or a required check failed; `degraded`
(200) when only optional checks failed, an outage the service survives and a restart would not
fix; `ready` (200) otherwise. The route replaces the framework's own `/health/ready`;
`/health/live` is unchanged.

## Event logging — `framework.commons.events`

```python
from framework.commons.events import configure_event_logging, get_event_logger

configure_event_logging("myapp", "INFO")          # JSON lines on the "myapp" logger tree
log = get_event_logger("myapp.orders")
log.info("order_accepted", order_id=42, payload=body)
log.bind(job="cleanup").warning("rows_skipped", count=3)
```

The first argument is a stable snake_case event name; keyword arguments become JSON fields of
the framework's line (with request and trace ids). Fields are passed through `redact`:

- values under a redaction key (`DEFAULT_REDACTED_KEYS`: bodies, documents, prompts, tokens,
  passwords, …; pass `redacted_keys=` to change them) become `"<redacted len=N>"`;
- strings over 256 characters become `"<truncated len=N>"`;
- DSN passwords (`scheme://user:***@host`) and URL signatures/tokens become `***` (`scrub`);
- containers are walked to depth 4 and 20 items.

`ScrubbingJSONFormatter` (installed by `configure_event_logging`) also scrubs formatted
tracebacks, since exception messages often carry a DSN or a presigned URL, and cuts every
traceback line longer than 200 characters to `<first 200 chars>… <truncated len=N>`, since an
exception message can carry a whole document (`configure_event_logging(...,
max_traceback_line=N)` or `ScrubbingJSONFormatter(max_line_length=N)` to change it).

## Span helpers — `framework.tracing.spans`

```python
from framework.tracing.spans import inject_http_headers, traced, traced_span

with traced_span("orders.load", **{"order.id": 42}):
    ...

@traced("orders.save")                     # default name: <module>.<function>
async def save(order): ...

await http.post(url, json=body, headers=inject_http_headers())
```

Spans use the process tracer (`init_tracing`), read at call time. Attributes are redacted like
log fields; an exception is recorded, marks the span failed and is re-raised.

`init_tracing(..., resource_attributes={"deployment.environment": "prod"},
export_timeout_s=3.0)` adds resource attributes next to `service.name` and bounds every export,
retries included; `shutdown_tracing()` exports what is queued and closes the provider within
about that time even when the collector is unreachable. The application runner reads both from
`FrameworkSettings.tracing_resource_attributes` and `otlp_export_timeout_sec`.

## CircuitBreaker — `framework.resilience`

For code that must **degrade** rather than raise while a dependency is down, for example a
cache that serves a rebuild. (`call_circuit_breaker` remains the decorator that raises
`CircuitOpenError`.)

```python
breaker = CircuitBreaker(threshold=3, open_s=30.0)
if breaker.allow():
    try:
        value = await redis.get(key)
    except RedisError:
        breaker.failure()
    else:
        breaker.success()
```

Opens after `threshold` consecutive failures, lets one probe through after `open_s`
(half-open), closes on its success, re-opens on its failure. One breaker per dependency per
process; not thread-safe.

## VersionedJsonCache — `framework.redis.versioned_cache`

Data derived from a configuration is cached under `(name, version)`. Whoever changes the
configuration bumps the version, so every replica sees the change on its next read: no TTL to
wait for, no invalidation to miss.

```python
cache = VersionedJsonCache(
    redis_client,
    key=lambda name, version: f"myapp:cfg:v{version}:{name}",
    breaker=redis_breaker,            # shared by every cache on that Redis
    budget_s=0.5,                     # per Redis call
    memo=True,                        # keep the decoded value in process while the version holds
    observe=lambda op, outcome, started: metrics.labels(op, outcome).inc(),
)
fields = await cache.get_or_build(
    "fields", version, ttl_s=3600, loader=load_fields, encode=to_json, decode=from_json
)
```

The cache never fails a read: a Redis error, a timeout, an open breaker or an undecodable value
falls back to the loader (`cache_unavailable` / `cache_corrupt` logged). Version `0` or lower
means "unknown" and is never cached. Use `memo=True` only for values nobody mutates.

## Testing helpers — `framework.testing.spans`

```python
import pytest
from framework.testing.spans import capture_spans


@pytest.fixture
def spans():
    with capture_spans() as captured:
        yield captured


async def test_save_is_traced(spans):
    await save(order)
    span = spans.one("orders.save")
    assert spans.children_of(span) == []
    assert "secret" not in spans.text()
```

## Not taken into the framework

| Component | Why it stays in the application |
|---|---|
| Leader-leased scheduler (Postgres advisory locks, a job-runs table) | Needs SQLAlchemy, a database schema and migrations in every user; the framework has no database layer |
| Epoch-unit rules (`EpochUnits` presets) | Business rules of one document model |
| SQL script splitter | Only useful with a database layer |
| Module registry / application context | Composition of one application |
| Per-service HTTP metrics (`<prefix>_http_requests_total{group}`) | Metric names and labels are the application's; use `OperationObserver` or `extra_middleware` |
