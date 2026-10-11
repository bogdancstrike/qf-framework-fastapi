# QF FastAPI specification

## Objective and scope

Create an independent sibling of the existing QF Flask framework at `qf_framework_fastapi`. Replace Flask/RESTX/gevent with FastAPI/Uvicorn and asyncio. Preserve decorator-based Kafka single/bulk workers, multi-topic Redis aggregation, dynamic endpoint mappings, call policies, tracing propagation and general utilities. The original project remains unchanged. The distribution is `qf-fastapi`; imports remain `framework`, so the two distributions must use separate environments.

## Async-first execution

Kafka consumption, publishing, acknowledgements, Redis aggregation, backoff, shutdown and HTTP lifecycle use awaitable APIs. Native `async def` handlers run on the event loop. Existing synchronous Kafka handlers run in bounded per-worker executors. Synchronous HTTP handlers run in Starlette's thread pool. No monkey patching or blocking network calls are allowed on the event loop. Async code must use async external clients. CPU-heavy work should run in a separate process/service.

## Functional requirements

1. Preserve `@kafka_handler`, `@kafka_aggregator`, `@retry_to_dlq`, `@rate_limit`, `@circuit_breaker`, and call policy imports. Accept sync and async worker functions with `(message_or_batch, consumer_name, metadatas)` arguments. Register one worker per topic and reject invalid worker definitions before connecting.
2. Support single messages, per-partition bulk batches flushed by size or maximum age, and deterministic aggregation by a nested correlation key. A worker may return a dict, a list of dicts, or `None` to intentionally filter an input.
3. Accept null/omitted endpoint model names, null/omitted top-level models and null named model definitions without requiring a body or schema validation; invalid JSON remains an error. Compile endpoint JSON at application construction. Resolve handler imports once, validate namespace/model/method/path declarations, create Pydantic request schemas, and expose OpenAPI and interactive documentation. Support typed path parameters, query parameters, sync/async handlers, and a documented Flask request compatibility adapter.
4. Use validated settings loaded from explicit arguments, environment, or dotenv in that precedence. Do not depend on an application-global `config.Config`; do not connect to services at import time. HTTP-only operation must require no Kafka or Redis.
5. Own connections and tasks through FastAPI lifespan. Propagate startup failure; expose runtime failure in readiness; drain bounded work and commit acknowledged progress before closing clients. Support a worker-only executable path.

## Reliability and delivery

- Use manual commits after processing and after acknowledgement of every output or DLQ record. Use an idempotent producer with all in-sync replica acknowledgements. These settings do not imply atomic Kafka transactions or exactly-once application side effects.
- Register fetched offsets before dispatch. Track their order rather than assuming offsets are consecutive. Never commit past an unfinished fetched record. Preserve completed progress across failed commit RPCs until the broker acknowledges it.
- Separate handler retries from transport retries. Handler attempts receive fresh copies of original inputs. Apply bounded exponential backoff with full jitter. Do not republish immediately to the input topic. Attempt counters reset on replay after process loss; document this limit.
- Preserve source topic, partition, offset, key, raw bytes, attempt count and failure category in DLQ envelopes. Malformed JSON and non-object input go to DLQ. Failed output/DLQ delivery must leave input uncommitted and make a terminal runtime failure visible.
- Bound queued and uncommitted records per partition and active jobs per worker. Pause/resume consumption at capacity and seek back any fetched records not admitted. Never create an unbounded number of waiting worker tasks. Commit gaps must not cause unbounded completion tracking.
- Drain or cancel revoked partition work with a deadline. Fence late completions by assignment generation and prohibit stale work from publishing further outputs. Cancellation cannot undo synchronous user side effects already running in a thread.
- Aggregation requires Redis only when aggregators are registered. Atomically store parts and claim complete aggregates. Use renewable token-checked leases, deterministic topic merge order and completion tombstones. Retain aggregate state until output or DLQ acknowledgement. Redis persistence, retention and Kafka retention determine the recovery boundary; cross-system atomicity is not promised.

## Observability

Provide `/health/live`, `/health/ready` and `/metrics`. Readiness must fail during startup, shutdown and terminal runtime failure. Export per-worker outcomes/retries/durations/inflight jobs, pending/uncommitted records, commit failures, partition lag, circuit state and event-loop lag. Use a separate Prometheus registry per application. Route labels must use templates and never raw request paths. Emit JSON logs with request ID and active trace ID without request bodies, credentials or arbitrary payloads. Propagate W3C trace headers through Kafka and HTTP. OTLP is optional, explicitly configured and flushed on shutdown.

## Compatibility and exclusions

Keep utility modules where practical. Replace unsafe expression evaluation with literal parsing. Remove global Redis singleton behavior and blocking KEYS enumeration. Flask extension objects, Flask request globals, WSGI startup and gevent execution are not retained. `load_framework_settings()` may discover `config.Config` when present; explicit settings require no such module. Document migration examples for every intentionally changed entry point and semantics. Filesystem distributed leases cannot provide robust fencing on arbitrary NFS deployments; Redis is the supported distributed locking backend.

## Validation and acceptance

Provide unit and regression tests, real Kafka/Redis end-to-end tests, HTTP load tests, worker throughput/latency benchmarks, bounded-capacity stress tests and failure injection. Cover output failure, DLQ failure, transient/permanent handler failures, commit failure, compacted offsets, rebalance fencing, partial batch timeout, aggregate duplicate/concurrent arrivals, shutdown and startup cleanup.

Compare the original framework and the new one with fixed payloads, partition counts, worker concurrency, handler work, producer settings and dependency versions. Report differences when equivalent delivery settings cannot be configured. Measure throughput, latency distribution, errors, delivered/duplicate/missing records, CPU/RSS and capacity where feasible. Distinguish synthetic measurements from real broker measurements and disclose warmup, repeats, hardware, environment and limitations. Do not claim a performance gain based solely on the architecture.

Write the comprehensive comparison at `../qf_flask_vs_qf_fastapi_report.md` relative to the project root. Include performance, reliability, code maintainability, extensibility, measured results and reproduction commands. Final acceptance requires a passing test suite, clean lint/build/import checks, runnable examples, complete operations documentation and an accurate record of any unexecuted tests.

## Implemented extension: replay ledger and tracing lineage

Optional Redis source-coordinate deduplication uses renewable claims and completion records with configurable TTL. It narrows completed replay duplication and retains a documented ACK-to-ledger window. Bulk outputs preserving IDs retain their source trace IDs and link producer spans to the batch. Distinct source traces are not rewritten into one trace. Four-process OTLP verification archives real Jaeger results. See Operations, Tracing and the comparison report for measured evidence and limits.

## Hardening and replica acceptance

- `kafka_handler(aggregator=True)` defaults to correlation field `id`; explicit dotted `aggregate_by` also selects aggregation. Retain the compatibility aggregator decorator. Reject bulk joins and missing/non-scalar/empty correlation keys before unsafe execution.
- Preserve every known ACK and pending producer receipt across local transport retries. Use the same delivery machinery for output and DLQ. A timeout must not cancel a still-valid send receipt and blindly resend it.
- Reuse Redis claim tokens across ambiguous transport replies. Completion scripts retain a token-valued receipt so a repeated completion succeeds for its owner. Batch independent ledger scripts and renew owned claims with one job heartbeat.
- Token-check SingleExecute completion, including work finishing before first renewal; reject incompatible sync/async clients. Preserve application errors on failed cleanup and keep borrowed clients application-owned.
- Terminal ETL errors fail liveness as well as readiness. Configure actual restart supervision; temporary readiness loss alone must not trigger a liveness failure.
- Verify independent replica processes sharing stable group/worker names: scale 2 → 4, kill an active worker, replace it, and recover every ID and source commit for single/bulk/join workers. Verify committed partial joins across total worker restart and safely idle excess consumers beyond partition count.
- No-loss restart acceptance is conditional on retained Kafka input and required Redis state. Disclose duplicate windows, finite join/ledger retention and infrastructure failover tests not executed. See [Scaling](Scaling.md).
