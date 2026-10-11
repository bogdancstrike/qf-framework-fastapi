# Implementation and validation tasks

Status is updated as work is implemented and verified. A checked item means its stated artifact or check exists; measured results belong in the comparison report.

## Reliability and replica follow-up

- [x] Reproduce and fix stale worker-circuit completions and cancelled half-open probes, including cancellation before task entry.
- [x] Retain complete join parts beyond active lease lifetime and propagate joined-handler failure to circuit policy.
- [x] Validate/snapshot explicit WorkerSpecs, correct cross-alias environment precedence and reject nonfinite settings and ambiguous endpoint declarations.
- [x] Preserve independent tracing/signal cleanup and primary errors through lifecycle failures.
- [x] Fail visibly on lost Redis processing ownership under a still-assigned partition, preserving source replay instead of silently stalling a gap.
- [x] Add 27 audit regressions and five repeated real-Kafka 5,000-message/five-worker scenarios with retries and restart.
- [x] Document extension points, state-machine invariants and resource ownership; profile and reduce repeated ACK metric label lookups.

- [x] Preserve confirmed/pending output and DLQ ACK receipts across retries; reproduce partial fanout, late ACK and bulk DLQ faults.
- [x] Expose custom/default aggregation through `kafka_handler`; reject conflicting bulk joins and missing/invalid correlation keys.
- [x] Make Redis claim/completion retries token-stable and replay-safe; pipeline source-ledger operations and use one heartbeat per job.
- [x] Add ACK, ledger replay/wait and aggregate-arrival observability.
- [x] Benchmark old/new FastAPI source-ledger processing at 10,000 records and two-branch joins at 10,000 correlations, three repeats/version.
- [x] Audit SingleExecute, common policies, instances namespace, Redis/Kafka resources, bytes/APDU boundaries and document compatibility changes.
- [x] Verify 2 → 4 independent replicas, active SIGKILL/replacement, retained partial joins after total worker restart and six consumers on four partitions.
- [x] Fix terminal-runtime liveness so configured Kubernetes probes can restart failed ETL pods.
- [x] Expand the examples README into a step-by-step tutorial; add a runnable competing-call SingleExecute example.
- [x] Document 20 future feature proposals separately from implemented scope.
- [x] Verify Python 3.15.0 final with 170 unit tests, the full 189-test live profile, native dependencies, pip check, package build and clean-wheel HTTP smoke.
- [x] Extend unit/build CI to 3.11–3.15 and real-service jobs to 3.12/3.15; preserve historical Python 3.12 benchmark provenance.

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

## Requested scale follow-up (completed)

- [x] Run 100,000-message matched ACK-original/native-async comparisons: four workloads, eight jobs, four partitions, three repetitions.
- [x] Update both report copies and README with completed-trial ratios, source-commit throughput, latency and failure evidence.
- [x] Add explicit output-versus-source-commit stall diagnostics and periodic progress to the benchmark driver.
- [x] Add management summary distinguishing throughput gains, resilience evidence and deployment limitations.

## Documentation and runnable pipeline follow-up

- [x] Add a documentation index, first-application tutorial, worker contract/cookbook, testing guide and troubleshooting reference.
- [x] Add an isolated HTTP → Kafka fanout → two branch workers → Redis join example with topic provisioner and bounded output-verification CLI.
- [x] Test the documented CLI/application against live services, including repeat provisioning, historical output isolation, schema errors and retry/DLQ behavior.
- [x] Validate local links and Python/JSON snippets, run the expanded 133-test live suite and retain existing performance measurements.
