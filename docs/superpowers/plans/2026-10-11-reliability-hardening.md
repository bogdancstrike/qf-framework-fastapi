# ACK, aggregation and idempotency hardening

**Goal:** Improve confirmed delivery and Redis retry semantics, expose aggregation through the familiar handler decorator, and measure costs with replay deduplication enabled.

**Design:** Keep at-least-once semantics and existing Redis key namespaces. Add per-output state so retries preserve ACKed and in-flight deliveries. Use stable claim tokens and completion receipts so ambiguous Redis replies can be retried safely. Pipeline independent Lua operations for bulk source-ledger work and renew its claims with one task per job. Keep every Lua operation individually atomic; a batch is not one cross-key transaction.

- [x] Reproduce acknowledged-output resend, late ACK timeout, lost Redis claim/completion replies, missing/default aggregate ID and bulk-aggregator configuration with focused regressions.
- [x] Extend `kafka_handler(aggregator=True, aggregate_by="id")`; retain `kafka_aggregator` compatibility and reject conflicting bulk aggregation. Validate dotted paths and correlation values.
- [x] Add cached Redis scripts, stable retry tokens, replay-safe completion receipts, batched ledger claims/completion/renewal and token-fenced cleanup. Retain namespace/TTL behavior and document crash windows.
- [x] Add a bounded ACK publisher shared by normal output and DLQ, preserving successful/pending sends across retries. Preserve per-source bulk trace lineage and expose ACK/idempotency/aggregate metrics.
- [x] Benchmark before/after with source deduplication enabled and real Kafka/Redis. Preserve old 1k/100k evidence and distinguish the new workload and code revision.
- [x] Run unit, E2E, crash, chaos, stress and real Jaeger checks. Update guides, examples, specs and both report copies, commit and push.
- [x] Recommend 20 optional follow-up features with their benefits, keeping them outside implementation until the user selects them.

## Replica and restart follow-up

Keep the original framework unchanged. Exercise independent OS processes with shared Kafka groups and Redis, using four partitions and unique test namespaces. Process tests approximate pod failures; they do not certify a Kubernetes cluster or broker/Redis failover.

- [x] Add `tests/support/pod_worker.py` with file-controlled handler stalls, process identity in outputs, and assignment snapshots. Use real `KafkaRuntime`, shared worker names and group, and optional source ledger.
- [x] Add `tests/test_horizontal_recovery.py`: scale 2 → 4 consumers during work, kill an active consumer, start a replacement, verify every ID and every source commit for single, bulk and join workers; also kill after committed partial join storage and complete the join after restart.
- [x] Reproduce the terminal-runtime liveness gap in `tests/test_api.py`, then return 503 only for a terminal ETL error from `/health/live`. Keep startup, temporary readiness loss and normal HTTP-only service live. Document native Kubernetes readiness versus restart behavior.
- [x] Publish raw per-scenario evidence with partition ownership, expected/observed IDs, duplicate counts, restart timing and committed offsets in `benchmarks/results/horizontal_recovery.json`.
- [x] Document group identity, worker contracts, partition limits, Redis persistence/TTL, external idempotency and deployment probes in `docs/Scaling.md`, the examples guide and both comparison reports. Run the full live suite, build, commit and push.

## Python 3.15 compatibility

- [x] Verify stable release and obtain an isolated 3.15 interpreter without replacing system Python or the existing benchmark environment.
- [x] Install the complete dev/tracing dependency set and record versions; reproduce and fix any framework compatibility failures without hiding unsupported dependencies.
- [x] Run unit tests, full Kafka/Redis/chaos/replica/Jaeger tests and wheel/sdist build on 3.15. Preserve previous benchmark provenance; do not infer a 3.15 speedup without measurements.
- [x] Add supported interpreter versions to CI, document exact executed results in both reports, commit and push, and check CI.
