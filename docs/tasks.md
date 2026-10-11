# Implementation and validation tasks

Status is updated as work is implemented and verified. A checked item means its stated artifact or check exists; measured results belong in the comparison report.

## Specification and baseline

- [x] Inspect the original Flask framework, worker registry, retry policies, coordinator, aggregation, dynamic endpoints and runner.
- [x] Record identified failure paths and define delivery requirements in `specs.md`.
- [x] Write architecture and implementation plan in this directory.
- [x] Create isolated sibling project, packaging metadata and validated settings without changing the original.
- [ ] Reproduce original coordinator/DLQ defects with executable regression probes.
- [ ] Record original dependency versions, source revision and environment for comparison.

## Async runtime

- [ ] Implement aiokafka consumer/producer lifecycle with bounded startup retry and cleanup.
- [ ] Implement fetched-offset coordinator with commit retry retention and numeric-gap support.
- [ ] Implement bounded queues, fair dispatch, async workers and per-worker sync executors.
- [ ] Flush bulk batches by size and oldest-record age.
- [ ] Acknowledge every output and DLQ before source completion.
- [ ] Separate handler retry from transport retry, use fresh inputs and full jitter.
- [ ] Apply job rate limits and circuit breaker recovery probes.
- [ ] Drain/cancel revoked work and fence old generations.
- [ ] Implement graceful shutdown and worker-only signal handling.

## Aggregation and policies

- [ ] Implement async Redis atomic merge/claim, lease renewal, release and completion tombstones.
- [ ] Make merge order deterministic and retain complete state until acknowledged output/DLQ.
- [ ] Add async Kafka convenience client and explicit Redis client ownership.
- [ ] Adapt call retry/rate/circuit policies for sync and async functions.
- [ ] Replace unsafe evaluation and audit legacy utility/locking behavior.

## API and observability

- [ ] Implement FastAPI factory and lifespan-owned runtime.
- [ ] Compile/validate endpoint mappings, Pydantic models and typed path parameters.
- [ ] Support sync and async HTTP handlers with documented request compatibility.
- [ ] Add liveness, readiness, metrics, sanitized request IDs and structured error responses.
- [ ] Add per-app metrics, loop lag, worker/partition gauges, JSON logging and trace propagation.
- [ ] Configure optional OTLP export and shutdown flushing.

## Tests and measured comparison

- [ ] Unit tests for settings, decorators, API schemas and policies.
- [ ] Coordinator tests for out-of-order completion, failed commit, numeric offset gaps and stale generations.
- [ ] Processing fault tests for handler, output and DLQ failures and malformed input.
- [ ] Real Redis tests for concurrent claims, retry/release, token ownership and tombstones.
- [ ] Real Kafka E2E tests for single/bulk/aggregate workers, retry/DLQ and committed offsets.
- [ ] Stress tests for capacity, slow workers, repeated transient failure and shutdown.
- [ ] Matched Flask/gevent and FastAPI HTTP load tests with warmup and repetitions.
- [ ] Matched original/new pipeline benchmarks for trivial, I/O and bulk workloads; report delivery-mode differences.
- [ ] Measure throughput, latency distribution, errors, missing/duplicate output, CPU and RSS where supported.
- [ ] Run lint, complete tests, package build and clean-wheel imports.
- [ ] Write `../../qf_flask_vs_qf_fastapi_report.md` from this docs directory with measured results, reliability evidence, maintainability/extensibility analysis and reproduction commands.

## Delivery documentation

- [ ] Complete README quickstart, runnable HTTP/worker examples and environment template.
- [ ] Document migration, settings, delivery boundaries, retry/DLQ recovery and deployment.
- [ ] Add container and CI checks, test profiles and benchmark commands.
- [ ] Review specification against final implementation; mark unexecuted/environment-limited checks explicitly.
