# Migrating an existing QF application

## Keep the declarations and configuration files

Install `qf-fastapi` in a new virtual environment. The distribution exposes `framework`, like the original; do not install both there. Copy your worker modules, `config.py` and `maps/endpoint.json`. The new runtime reads existing uppercase `Config` values through `FrameworkApp.from_config(Config)` or `load_framework_settings(config=Config)`.

The worker decorators retain their names, parameters and `(message, consumer_name, metadatas)` function arguments. Single handlers return dict/None/list; bulk handlers receive a list. Aggregators retain `aggregate_by`, input topics and timeout. Runtime policy decorators work above or below worker registration. Function policies also update registered callables when applied outside registration.

Keep `get_tracer`, `init_tracing`, `inject_trace_headers` and `extract_context_from_headers`. Application spans still use `get_tracer().start_as_current_span(...)` and the standard span methods. Tracing is explicitly enabled through settings; `QSINT_ENABLE_TRACING` and `ENABLE_TRACING` are recognized. An OTLP endpoint is required when tracing is enabled. TLS is enabled unless explicitly disabled for a local collector.

## Startup changes

Old WSGI startup:

```python
handles = FrameworkApp(settings).run()
handles.app.run(host="0.0.0.0", port=5000)
```

New ASGI startup (`main.py`):

```python
from pathlib import Path
from config import Config
from framework.app import FrameworkApp

framework = FrameworkApp.from_config(Config, app_root=Path(__file__).parent)
app = framework.build_app()
```

```bash
uvicorn main:app --host 0.0.0.0 --port 5000 --workers 1
```

Or call `framework.serve()` from the main guard. `run()` still returns handles but defers resource startup to ASGI lifespan; no daemon ETL thread is created. `await framework.shutdown()` stops the runtime. Low-level `framework.etl.framework_etl.start(...)` is now async and must be awaited. For worker-only execution, set `enable_api=False` and call `serve()`, or await `run_worker()` from an existing event loop on Unix.

Do not call `gevent.monkey.patch_all()`. Sync Kafka workers are offloaded into bounded worker-specific executors. `async def` workers run directly; use async clients such as `httpx.AsyncClient`, async DB drivers and async Redis inside them. Replacing `def` with `async def` while retaining `requests` or `time.sleep` blocks every async worker and HTTP request in that process.

## Dynamic endpoints

The existing `namespaces`, `models`, `endpoints`, `exec_method`, `model_name`, `operation_name`, `request_method` and `api_url` fields remain. Flask-style `<int:item_id>`, `<string:name>`, `<uuid:id>` and `<path:value>` parameters are compiled to FastAPI path parameters. Full namespace paths are not duplicated. Schema validation also applies to PATCH. Unknown model names/types, duplicate routes and missing handlers fail construction rather than surfacing on a first request.

Sync handlers keep:

```python
def handler(app, operation, request, item_id):
    payload = request.get_json()
    query = request.args.get("q")
    return {"id": item_id, "payload": payload}, 201
```

`request.json` (property), `get_json()`, `args`, `headers`, `method`, `url` and `path` are provided by a compatibility adapter. Dict responses, `(body, status)` and `(body, status, headers)` are supported. The `app` argument is a FastAPI app: access shared objects through `app.state`, and settings through `app.state.settings`. Flask globals, extensions, sessions and `app.config` require migration. Form/file uploads should use native FastAPI routes; the compatibility adapter is intended for JSON mappings.

Native async handlers receive a Starlette Request:

```python
async def handler(app, operation, request, item_id):
    payload = await request.json()
    query = request.query_params.get("q")
    return {"id": item_id, "payload": payload}
```

Request validation errors use FastAPI's 422 format; namespace docs use `/docs`. Trailing slash mismatch follows FastAPI redirect behavior. Authentication and authorization remain application/deployment responsibilities. Add native routes and dependencies through the synchronous `init_app` hook or on the returned FastAPI app.

## Deliberate reliability changes

`KAFKA_COMMIT_STRATEGY=before`, `KAFKA_BULK_OUTPUT_SYNC=False` and fire-and-forget output are not supported. All paths await acknowledgements. Handler retries remain configured with `@retry_to_dlq`, but run locally with async backoff rather than immediately republishing to the input topic. The counter is kept in memory; it resets after process loss and no longer changes application payload `retry_count` fields. DLQ messages are diagnostic envelopes; consumers must read `raw_base64` or the retained `aggregate` snapshot when replaying.

Missing aggregate correlation keys are routed to DLQ. Topic merge precedence is deterministic in `topics_in` order. Completed correlations are suppressed for the aggregation timeout window; use unique correlation IDs for distinct logical joins. Redis source-record idempotency is optional and TTL-limited. Stable source-derived IDs are used when inputs lack an `id`. Do not interpret producer idempotence or Redis tombstones as transactional exactly-once processing.

## Helper clients and utilities

`RedisUtils` retains helper names but is async, explicitly owned and not a singleton. Use `async with RedisUtils(...)` or `await close()`. Enumeration uses SCAN. `KafkaClient` methods are async; publishing uses one producer per `async with` context. The convenience consumer reads fetched records without auto-committing; it is a diagnostic/read helper, not a durable worker substitute.

`SingleExecute` supports both sync Redis clients for sync functions and async Redis clients for async functions. Redis is the supported distributed lock backend; the unsafe expiring filesystem lease implementation is not carried forward. Use an external scheduler for periodic execution. Ownership loss cannot undo user side effects; use downstream fencing/idempotency for critical actions.

Literal helper parsing uses `ast.literal_eval`, replacing expression execution. Full-width integer-to-byte conversion and nested empty-path lookup are corrected. APDU/hex helpers retain their principal names, but are legacy domain utilities, not an independently certified protocol implementation.
