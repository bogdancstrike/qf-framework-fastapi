# QF FastAPI Migration Implementation Plan

> **For agentic workers:** Execute inline in the requested sibling project. Follow the checked tasks and their regression tests; the source Flask project is read-only.

**Goal:** Preserve QF workers, aggregation, dynamic HTTP endpoints, and utilities while replacing Flask/gevent with FastAPI and an asynchronous, failure-aware pipeline.

**Architecture:** FastAPI lifespan owns an asyncio Kafka runtime. A bounded scheduler dispatches async workers or synchronous workers to per-worker executors. A coordinator tracks fetched records in order and retains commit progress until broker acknowledgement. Redis aggregation uses atomic claims, renewable leases, and completion tombstones.

**Tech Stack:** Python 3.11+, FastAPI/Pydantic 2, Uvicorn, aiokafka, redis-py, Prometheus, optional OpenTelemetry OTLP.

## File map and execution

- [ ] Settings and packaging: `pyproject.toml`, `requirements.txt`, `src/framework/config/settings.py`. Validate Kafka timeouts, queue limits, retry budgets, environment aliases and explicit overrides. Test `tests/test_settings.py` with `python -m pytest tests/test_settings.py`.
- [ ] Runtime: `src/framework/etl/{framework_etl,coordinator,policies,processing}.py`. Register every fetched record before dispatch, fence revoked assignments, acknowledge every output and DLQ, retry transport failures separately from application failures, enforce bounded queues and worker concurrency, flush partial batches, drain shutdown. Test `tests/test_runtime.py` and `tests/test_processing.py` using injected broker fakes, including failed commits and compacted offsets.
- [ ] Aggregation and clients: `src/framework/redis/{aggregation,redis_utils}.py`, `src/framework/streams/kafka_client.py`. Use Redis atomic scripts for merge/claim/renew/finish/release, deterministic merge order, explicit client ownership, SCAN instead of KEYS, async producer lifecycle. Test `tests/test_aggregation.py` against ephemeral Redis.
- [ ] API and lifecycle: `src/framework/api/{server,dynamic}.py`, `src/framework/app/runner.py`. Compile endpoint mappings once, generate Pydantic body models and typed path arguments, support sync/async handlers, structured errors, readiness/liveness and startup failure propagation. Test `tests/test_api.py` with TestClient.
- [ ] Observability and call policies: `src/framework/observability.py`, `src/framework/tracing/tracing.py`, `src/framework/commons/logger.py`, decorators. Per-app metrics registry, bounded labels, request IDs and trace IDs, explicit Kafka propagation, async-safe retries/circuit/rate policies and renewable single-execution lock. Test `tests/test_policies.py`.
- [ ] Documentation and deployment: `README.md`, `docs/{Analysis,Migration,Operations,Architecture}.md`, `examples/`, `Dockerfile`, `compose.yaml`, `.github/workflows/ci.yml`. Include runnable examples, settings reference, delivery boundaries, retry/DLQ recovery, production deployment and profiling guidance. Run complete pytest suite, Ruff, wheel build, clean-wheel import smoke test and HTTP/pipeline benchmark before finishing.

## Evidence and acceptance

The original coordinator consumes completed offsets before a commit RPC succeeds; `0, 2` cannot advance past the missing `1`; malformed input bypasses processing and DLQ; single and aggregate output paths use `output_sync=False`; no retry-policy branch ignores DLQ send failure; no rebalance listener exists; import requires application `config.Config` and initializes Redis eagerly; runner shutdown is a warning only. Preserve these findings in `docs/Analysis.md` and cover corrected behavior with regression tests.

Acceptance requires no Flask/gevent imports or dependencies, HTTP-only startup without Kafka/Redis, sync/async workers, mapping validation, ACK-gated completion, retained failed commits, retry jitter and terminal failure visibility, stale-generation fencing, bounded memory, graceful resource cleanup, real Redis script coverage, buildable distribution and honest benchmark limitations.
