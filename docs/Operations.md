# Operations and recovery

For a first application, use the [getting-started guide](GettingStarted.md) and [complete pipeline example](../examples/pipeline/README.md). See [worker contracts](Workers.md) before selecting concurrency and idempotency boundaries, and [troubleshooting](Troubleshooting.md) when diagnosing an incident.

## Install and run

Use Python 3.11 or later and an environment separate from the Flask distribution: both packages export `framework`. Install `pip install '.[tracing]'` from a checkout, or build a wheel with `python -m build`. Application configuration and worker modules belong to your application, outside the installed framework package.

Run `uvicorn examples.app:app --host 0.0.0.0 --port 4000 --workers 1` for the bundled HTTP example. Set `QSINT_ENABLE_ETL=true` after starting the development compose stack to enable its Kafka workers. Each ASGI process starts a consumer; replicas sharing a consumer group divide partitions. Avoid development reload for capacity measurements. For workers without HTTP set `QSINT_ENABLE_API=false` and call `FrameworkApp(settings).serve()` from a main guard. Worker-only mode exits on terminal runtime failure; supervise it with your process manager.

The Dockerfile installs the package and bundled example as a non-root user. The compose file provides test infrastructure; it does not launch the application container. Its advertised external broker address is localhost, suitable for applications running on the host. A container on the compose network should use `kafka:9092`, `redis://redis:6379/0`, and `http://jaeger:4317`.

## Configuration

`FrameworkSettings` validates types and ranges. Direct constructor arguments override `QSINT_` environment values, which override `.env`. `FrameworkApp.from_config(Config)` additionally loads supported uppercase legacy fields as fallback defaults. Lists/dicts in environment variables must be JSON. See [Migration.md](Migration.md) for exact legacy behavior and the generated [configuration reference](Configuration.md) for all fields.

Key starting points:

| Setting | Default | Purpose |
|---|---:|---|
| `pending_max_per_tp` | 750 | Bound fetched records awaiting successful commit, including completed records behind a gap |
| `poll_max_records` | 500 | Maximum records returned by one poll |
| `poll_timeout_ms` | 25 | Application poll wait |
| `fetch_max_wait_ms` | 50 | Broker fetch wait; shorter waits reduce backpressure/refetch latency, increasing idle request traffic |
| `commit_tick_sec` | 0.2 | Commit cadence; affects replay window and bounded-buffer turnover |
| `output_ack_timeout_sec` | 30 | Deadline for one output/DLQ transport attempt |
| `retry_attempts` | 3 | Handler attempt budget unless overridden by decorator |
| `transport_attempts` | 5 | Producer/Redis retry budget; also consecutive commit failure budget |
| `aggregate_lease_sec` | 30 | Renewable claim lifetime |
| `idempotency_enabled` | false | Optional Redis source-coordinate replay ledger |
| `idempotency_ttl_sec` | 86400 | Finite ledger retention window |

`max_workers` belongs to each worker decorator. Bulk capacity is measured in jobs: eight active jobs can each contain 32 records. The per-partition uncommitted bound still applies. Larger buffers and batches can improve throughput while increasing memory, queueing latency and replay volume. Tune using your own payload sizes and downstream limits.

Kafka TLS/SASL options are exposed in settings; use secret injection for credentials. Local compose uses plaintext. The framework does not configure broker replication, ACLs, topic retention, application authentication or ingress authorization.

## Delivery contract

Successful handler output, intentional filtering (`None`), acknowledged diagnostic DLQ, durable incomplete aggregate storage, and completed-ledger skips can mark input complete. The coordinator commits only a completed prefix of actual fetched records. Failed commits keep their candidate until acknowledgement. The producer enables idempotence and `acks=all`; on this test stack that still means only one broker, not replicated durability.

Processing is at least once. Kafka output and source commits are separate operations; a crash between them can duplicate output. Kafka producer idempotence does not cover process restarts or business effects. The optional Redis ledger narrows replay duplication after its completion write, with a remaining output-ACK-to-ledger-write window. It identifies source topic/partition/offset, not arbitrary repeated business IDs at different offsets. Its TTL must exceed your intended replay horizon. Neither feature provides cross-system exactly-once transactions.

Aggregators acknowledge incomplete parts after Redis storage. Redis AOF `everysec` in compose can lose recent writes on a machine/persistence failure; that is not a Kafka offset recovery guarantee. Production Redis durability, failover, backups, memory policy and retention must match the business requirements. Lease ownership is checked before publishing and completion, but cannot retract output already accepted by Kafka or side effects already performed by user code. Completion tombstones suppress repeated correlations for the worker's aggregation timeout; use unique correlation IDs for independent joins. Incomplete parts expire without automatic expiry DLQ.

## Failure and retry behavior

| Failure | Runtime behavior | Operator action |
|---|---|---|
| Handler exception | Fresh input copy, bounded local backoff/full jitter, then acknowledged DLQ | Inspect error category and original input; fix handler or invalid data |
| Malformed JSON / missing aggregate key | Diagnostic DLQ without handler execution | Fix producer contract |
| Output or DLQ unavailable | Separate transport retries; budget exhaustion fails readiness and leaves source replayable | Restore dependency, restart consumer |
| Commit failure | Retain completed prefix and retry on subsequent ticks; repeated failure terminates runtime | Restore group/broker availability, restart |
| Redis unavailable or ownership lost | No successful aggregate/ledger completion; failure/replay path | Restore Redis, inspect retained parts and leases |
| Rebalance | Stop dispatch, bounded drain, cancel, commit completed prefix, discard old generation | Usually automatic; inspect sustained rebalance churn |
| SIGTERM | Stop fetching, bounded drain, cancel unfinished tasks, final commit, close clients and flush tracing | Give the process manager enough termination grace |
| SIGKILL | No cleanup; restart from committed offsets | Expect replay, apply downstream idempotency |

Terminal ETL errors make both `/health/live` and `/health/ready` return 503. Configure Kubernetes liveness to restart failed containers; readiness alone only removes HTTP Service traffic and does not restart or pause Kafka workers. Startup/temporary readiness loss alone keeps liveness healthy. Retry counters are in memory and reset on crash/replay. A synchronous function already executing in a thread cannot be forcibly interrupted; protect its business effects with downstream idempotency/fencing. Stop deadlines apply to individual phases/resources, so total teardown can exceed one `shutdown_timeout_sec`; allow sufficient grace for multiple phases plus exporter flush. The [scaling guide](Scaling.md) includes a deployment template and executed multi-process restart evidence.

DLQ records contain `source`, `worker`, `attempts`, `error.type`, `raw_base64`, and source key bytes. Aggregate failures additionally retain the merged snapshot. Fix the fault first, decode and validate the envelope, and republish the desired application payload to an isolated replay topic or the original input with an explicit replay policy. Republishing creates new offsets and bypasses source-coordinate deduplication. Never blindly feed diagnostic envelopes back as application payloads.

## Monitoring

Scrape `/metrics` separately for each process. `/health/live` checks HTTP availability and terminal ETL failure; `/health/ready` checks runtime readiness. Observe `qf_runtime_ready`, `qf_failures_total`, `qf_retries_total`, `qf_messages_total`, `qf_job_duration_seconds`, `qf_pending_records`, `qf_uncommitted_records`, `qf_inflight_jobs`, `qf_consumer_lag`, `qf_circuit_open`, and `qf_event_loop_lag_seconds`. New `qf_output_acks_total`, `qf_output_ack_duration_seconds`, `qf_idempotency_total` and `qf_aggregate_parts_total` expose acknowledged transport and claim/replay outcomes. Lag is broker highwater minus last acknowledged commit, not pure handler queue length; it appears after a first successful commit. Scheduler lag is a coarse wakeup-delay indicator, not a separate calibrated loop monitor.

Alert on failed readiness, increasing lag with no successful completions, sustained capacity saturation, transport/commit failures, and long-open circuits. HTTP labels use route templates. Worker/topic/partition labels have deployment-dependent cardinality; do not register unbounded temporary workers/topics in long-running production processes. JSON logs include active trace and request IDs. Application logs and user spans must avoid sensitive payloads.

See [Tracing.md](Tracing.md) for OTLP settings, a verified Jaeger trace, bulk context semantics and propagation examples.

## Validation boundaries

The included tests exercise one local broker, local Redis, process failure and temporary broker pause. Multi-broker leader failover, Redis node failure/data loss, TLS/SASL end-to-end, long soak tests, geographically distributed services and CPU-heavy business workloads require deployment-specific validation. The [comparison report](qf_flask_vs_qf_fastapi_report.md) records measured results and limitations.
