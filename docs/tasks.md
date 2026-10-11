# Implementation and validation tasks

Status is updated as work is implemented and verified. A checked item means its stated artifact or check exists; measured results belong in the comparison report.

## Specification and baseline

- [x] Inspect the original Flask framework, worker registry, retry policies, coordinator, aggregation, dynamic endpoints and runner.
- [x] Record identified failure paths and define delivery requirements in `specs.md`.
- [x] Write architecture and implementation plan in this directory.
- [x] Create isolated sibling project, packaging metadata and validated settings without changing the original.
- [x] Reproduce original coordinator/DLQ defects with executable regression probes.
- [x] Record original dependency versions, source revision and environment for comparison.

## Async runtime

- [x] Implement aiokafka consumer/producer lifecycle with bounded startup retry and cleanup.
- [x] Implement fetched-offset coordinator with commit retry retention and numeric-gap support.
- [x] Implement bounded queues, fair dispatch, async workers and per-worker sync executors.
- [x] Flush bulk batches by size and oldest-record age.
- [x] Acknowledge every output and DLQ before source completion.
- [x] Separate handler retry from transport retry, use fresh inputs and full jitter.
- [x] Apply job rate limits and circuit breaker recovery probes.
- [x] Drain/cancel revoked work and fence old generations.
- [x] Implement graceful shutdown and worker-only signal handling.

## Aggregation and policies

- [x] Implement async Redis atomic merge/claim, lease renewal, release and completion tombstones.
- [x] Make merge order deterministic and retain complete state until acknowledged output/DLQ.
- [x] Add async Kafka convenience client and explicit Redis client ownership.
- [x] Adapt call retry/rate/circuit policies for sync and async functions.
- [x] Replace unsafe evaluation and audit legacy utility/locking behavior.

## API and observability

- [x] Implement FastAPI factory and lifespan-owned runtime.
- [x] Compile/validate endpoint mappings, Pydantic models and typed path parameters.
- [x] Support sync and async HTTP handlers with documented request compatibility.
- [x] Add liveness, readiness, metrics, sanitized request IDs and structured error responses.
- [x] Add per-app metrics, loop lag, worker/partition gauges, JSON logging and trace propagation.
- [x] Configure optional OTLP export and shutdown flushing.

## Tests and measured comparison

- [x] Unit tests for settings, decorators, API schemas and policies.
- [x] Coordinator tests for out-of-order completion, failed commit, numeric offset gaps and stale generations.
- [x] Processing fault tests for handler, output and DLQ failures and malformed input.
- [x] Real Redis tests for concurrent claims, retry/release, token ownership and tombstones.
- [x] Real Kafka E2E tests for single/bulk/aggregate workers, retry/DLQ and committed offsets.
- [x] Stress tests for capacity, slow workers, repeated transient failure and shutdown.
- [x] Matched Flask/gevent and FastAPI HTTP load tests with warmup and repetitions.
- [x] Matched original/new pipeline benchmarks for trivial, I/O and bulk workloads; report delivery-mode differences.
- [x] Measure throughput, latency distribution, errors, missing/duplicate output, CPU and RSS where supported.
- [x] Run lint, complete tests, package build and clean-wheel imports.
- [x] Write `../../qf_flask_vs_qf_fastapi_report.md` from this docs directory with measured results, reliability evidence, maintainability/extensibility analysis and reproduction commands.

## Delivery documentation

- [x] Complete README quickstart, runnable HTTP/worker examples and environment template.
- [x] Document migration, settings, delivery boundaries, retry/DLQ recovery and deployment.
- [x] Add container and CI checks, test profiles and benchmark commands.
- [x] Review specification against final implementation; mark unexecuted/environment-limited checks explicitly.

## Additional completed requirements

- [x] Optional TTL-limited source-coordinate deduplication ledger and real SIGKILL replay tests, both with and without the ledger.
- [x] Explicit dedicated-broker outage/restart test; no unrelated containers affected.
- [x] Four-process OTLP test queries real Jaeger storage and verifies downstream trace IDs and parent links.
- [x] Bulk output/DLQ regressions retain distinct per-source trace IDs.
- [x] Null/omitted endpoint models, top-level models null and null named definitions; tests for sync/async GET/POST/PUT/PATCH/DELETE.
- [x] Larger-burst benchmark and cProfile diagnosis; configurable lower fetch wait, measured before/after results, explicit original timeout trials.
- [x] Public GitHub repository, periodic commits/pushes, CI workflow and comprehensive GitHub README with worker/commit diagrams.
- [x] Tracked raw benchmark measurements and comparison report in docs and requested workspace parent.
- [x] .gitignore excludes secrets, virtual environments, logs, profiling and build artifacts while preserving examples/results.

## Validation limits and follow-up work

Completed checkboxes describe implemented artifacts and executed checks, not universal delivery/performance guarantees. `benchmarks/results/validation.json` records final local checks; GitHub Actions records Python 3.11/3.12/3.13 checks and live-service validation. Reports retain historical measurements and timeouts. Unexecuted production-specific validation: multi-broker failover, Redis node/data loss, TLS/SASL end-to-end, sustained multi-hour soak, independent load generator and application business effects. These remain deployment acceptance tasks, not silently passing checks.
