# QF FastAPI + asyncio

Async-first successor to the QF Flask worker framework. Keep the worker decorators, `config.Config`, endpoint JSON format and tracing helpers; use FastAPI, aiokafka and async Redis underneath. Synchronous workers remain supported through bounded executors.

Requires Python 3.11+. Install in a **separate environment** from the original `qf`: both distributions expose `framework` imports.

## Run the example

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev,tracing]'
uvicorn examples.app:app --port 4000
curl -X POST http://localhost:4000/examples/echo/42 \
  -H 'Content-Type: application/json' -d '{"text":"hello"}'
```

The example starts HTTP without Kafka or Redis. Open `/docs`, `/openapi.json`, `/health/live`, `/health/ready` and `/metrics`.

To enable workers:

```bash
docker compose -p qf-fastapi-test up -d --wait
cp .env.example .env
# Set QSINT_ENABLE_ETL=true in .env
uvicorn examples.app:app --port 4000
```

The compose file is single-broker development/test infrastructure. For a containerized application on its network, set `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` and `QSINT_REDIS_URL=redis://redis:6379/0`.

## Existing configuration

```python
from pathlib import Path
from config import Config  # your existing uppercase settings
from framework.app import FrameworkApp

framework = FrameworkApp.from_config(Config, app_root=Path(__file__).parent)
app = framework.build_app()  # uvicorn main:app
```

`WORKER_NAME`, `WORKER_MODULE`/`WORKER_MODULES`, `KAFKA_BOOTSTRAP_SERVERS`, `ERROR_TOPIC`, Redis host/port/db/password, and the documented legacy tuning keys are supported. `load_framework_settings()` also discovers `config.Config` when present. Explicit overrides take precedence over environment/dotenv, which take precedence over Config defaults. New settings use `QSINT_` environment names. List/dict values use JSON.

You may also pass `FrameworkSettings` directly. `framework.run()` returns handles with the FastAPI app; resources start in its ASGI lifespan. `framework.serve()` starts Uvicorn or worker-only mode. See [migration](docs/Migration.md) for the changed startup call and async Redis/Kafka helpers.

## Workers

```python
from framework.decorators import kafka_handler, retry_to_dlq


@retry_to_dlq(max_attempts=3, dlq_topic="text.dlq")
@kafka_handler(name="normalize", topics_in=["text.in"], topics_out=["text.out"], max_workers=16)
async def normalize(message, consumer_name, metadatas):
    message["text"] = message["text"].strip()
    return message
```

The same declaration accepts `def` workers. Async workers must use awaitable clients for network I/O. Keep CPU-heavy work in separate processes/services. One worker owns each input topic; replicas scale through Kafka partitions and a shared consumer group. Handler completion order may differ from input order; consumers needing ordered output should use an appropriate concurrency/partitioning design.

For batches, set `bulk_mode=True`, `batch_size=32`, `batch_timeout_ms=25`; the function receives a list. For joins use `@kafka_aggregator(topics_in=[...], aggregate_by="id", ...)`. See [examples/workers.py](examples/workers.py).

## Delivery, retries and idempotency

Inputs are committed only after all output or DLQ acknowledgements. The producer uses idempotence and `acks=all`. Handler retries use fresh input copies, capped exponential backoff and full jitter. Broker/Redis operations have a separate retry budget. Failed commits retain progress. Exhausted transport/commit budgets make readiness fail and preserve uncommitted inputs for replay.

This is **at-least-once delivery**. A crash between output acknowledgement and source commit can duplicate an output. Preserve stable message IDs and make downstream effects idempotent. Optional `QSINT_IDEMPOTENCY_ENABLED=true` adds a Redis source-record ledger and renewable claims to skip completed replays. It still has an ACK-to-ledger crash window and a finite TTL; it does not promise exactly-once business effects. Correlation-based aggregation has its own completion tombstones.

Malformed input goes to a diagnostic DLQ envelope with original bytes, source coordinates, attempts and error type. Retry attempts restart after process loss. Redis joins acknowledge incomplete parts after storage and require durable Redis persistence. Read [operations and recovery](docs/Operations.md) before production use.

## Observability

JSON logs include request IDs and active trace IDs. Prometheus metrics cover worker outcomes/retries/duration, queues, uncommitted records, inflight jobs, lag, circuits, commit failures and event-loop delay. HTTP metric labels use route templates.

Tracing helper names remain unchanged:

```python
from framework.tracing import get_tracer, inject_trace_headers, extract_context_from_headers

with get_tracer().start_as_current_span("my.operation") as span:
    span.set_attribute("application.phase", "enrich")
```

Install `.[tracing]`, set `QSINT_ENABLE_TRACING=true` and `QSINT_OTLP_ENDPOINT`; local insecure collectors additionally need `QSINT_OTLP_INSECURE=true`. HTTP and Kafka context propagation is explicit. Tracing is off by default.

## Validate and benchmark

```bash
python -m pytest -q
QF_INTEGRATION=1 python -m pytest tests/test_e2e.py -v
ruff check . && ruff format --check .
python -m build
python benchmarks/compare_pipelines.py --messages 10000 --repeats 5
python benchmarks/compare_http.py --requests 10000 --repeats 5
```

Comparison scripts require the original sibling project's `.venv` and its dependencies, including gevent. They execute the original source without changing it. The original ACK-gated benchmark variant is explicitly labeled as a harness patch. Benchmark workloads use isolated temporary topics; raw measurements and dependency snapshots are in [benchmarks/results](benchmarks/results). Run suites sequentially to avoid interference.

The comprehensive comparison is [qf_flask_vs_qf_fastapi_report.md](../qf_flask_vs_qf_fastapi_report.md). Read its methodology and limitations before treating measured throughput as a production capacity estimate.

## Documentation

- [Specification](docs/specs.md), [architecture](docs/architecture.md), [task status](docs/tasks.md)
- [Migration and compatibility](docs/Migration.md)
- [Operations, configuration and failure recovery](docs/Operations.md)
- [Original code analysis](docs/Analysis.md)
- [Test and benchmark guide](benchmarks/README.md)
