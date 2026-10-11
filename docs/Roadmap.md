# Twenty proposed features to choose from

These are recommendations, not committed implementation scope. Existing source-coordinate deduplication, bounded handler/transport retries, Redis correlation joins and Prometheus/OTLP tracing remain the current baseline. Select the features that match your application before adding complexity.

| # | Feature | Benefit and boundary |
|---|---|---|
| 1 | Business-key idempotency with payload fingerprints | Suppress repeated logical requests at new Kafka offsets; reject conflicting payloads for one key. Requires explicit key, retention and replay policy. |
| 2 | Database outbox/inbox adapters | Commit business state and publication intent together; deduplicate consumed business actions in the database transaction. Select supported databases first. |
| 3 | Kafka transactional worker mode | Atomically publish Kafka outputs and commit source offsets for pure Kafka stages. Does not make Redis/database/HTTP effects transactional. |
| 4 | Durable delayed-retry topics | Preserve attempt history across restarts and schedule retry without holding worker slots. Requires retry topic provisioning and ordering policy. |
| 5 | Transient/permanent error classification | Retry recoverable failures and send contract violations directly to DLQ with structured reason codes. |
| 6 | Join-expiry diagnostics | Emit a diagnostic showing missing branches when retained partial joins expire, instead of silent expiry. Requires durable expiry scheduling, not unreliable keyspace notifications alone. |
| 7 | Ordered execution per business key | Preserve account/order sequence while processing unrelated keys concurrently. Requires fairness, bounded key state and partitioning rules. |
| 8 | Byte-based memory and payload limits | Bound memory for large messages as well as record count; reject/route oversized payloads clearly. |
| 9 | Typed worker contracts | Validate input/output schemas with Pydantic, optionally Avro/Protobuf and registry compatibility. |
| 10 | Pipeline graph validation and diagrams | Detect missing stages, cycles and invalid topic contracts before deployment; generate a diagram from declarations. |
| 11 | Safe DLQ replay CLI | Preview/validate payloads, select failures, rate-limit replay and retain an audit record. |
| 12 | Operations CLI | Inspect group assignments, source offsets, lag and topic durability settings without custom scripts. Offset mutation requires an explicit operator workflow. |
| 13 | Worker resource dependency injection | Lifespan-owned database/HTTP clients injected into workers without import globals; easier test substitution. |
| 14 | Distributed admission quotas | Share downstream rate/concurrency budgets across pods rather than multiplying process-local limits. |
| 15 | Adaptive concurrency | Adjust job concurrency using latency, errors and backlog, with explicit minimum/maximum and stability controls. |
| 16 | Conditional output routing | Route individual results to declared topics based on business conditions, preserving ACK and tracing contracts. |
| 17 | Durable pipeline job-status API | Query accepted/running/completed/failed jobs without consuming result topics; define retention and status authority. |
| 18 | Unified OTLP logs and metrics | Export correlated logs, metrics and traces through one collector. Current traces already use OTLP; logs are JSON and metrics Prometheus. |
| 19 | Multi-parent join trace links | Retain every arrival's span context and link all parents, including independently rooted branches. Do not force unrelated traces into one ID. |
| 20 | Process-pool CPU workers | Isolate CPU-intensive transforms from the event loop/GIL, with bounded queues, serialization and shutdown contracts. |

Recommended first choices are **1, 4, 5, 6 and 11**: business idempotency, durable retries, error classification, join expiry and controlled replay address concrete operational gaps. Move **2** near the top when database effects are central. Features 3/7/15/17 have larger semantic and maintenance costs and should start from a concrete workload.

Reference designs: [Debezium outbox event router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html), [aiokafka transactions](https://aiokafka.readthedocs.io/en/stable/producer.html#transactional-producer), [OpenTelemetry signals](https://opentelemetry.io/docs/concepts/signals/). These references support the mechanisms; prioritization is an engineering recommendation for this framework.
