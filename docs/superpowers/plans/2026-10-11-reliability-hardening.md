# ACK, aggregation and idempotency hardening

**Goal:** Improve confirmed delivery and Redis retry semantics, expose aggregation through the familiar handler decorator, and measure costs with replay deduplication enabled.

**Design:** Keep at-least-once semantics and existing Redis key namespaces. Add per-output state so retries preserve ACKed and in-flight deliveries. Use stable claim tokens and completion receipts so ambiguous Redis replies can be retried safely. Pipeline independent Lua operations for bulk source-ledger work and renew its claims with one task per job. Keep every Lua operation individually atomic; a batch is not one cross-key transaction.

- [ ] Reproduce acknowledged-output resend, late ACK timeout, lost Redis claim/completion replies, missing/default aggregate ID and bulk-aggregator configuration with focused regressions.
- [ ] Extend `kafka_handler(aggregator=True, aggregate_by="id")`; retain `kafka_aggregator` compatibility and reject conflicting bulk aggregation. Validate dotted paths and correlation values.
- [ ] Add cached Redis scripts, stable retry tokens, replay-safe completion receipts, batched ledger claims/completion/renewal and token-fenced cleanup. Retain namespace/TTL behavior and document crash windows.
- [ ] Add a bounded ACK publisher shared by normal output and DLQ, preserving successful/pending sends across retries. Preserve per-source bulk trace lineage and expose ACK/idempotency/aggregate metrics.
- [ ] Benchmark before/after with source deduplication enabled and real Kafka/Redis. Preserve old 1k/100k evidence and distinguish the new workload and code revision.
- [ ] Run unit, E2E, crash, chaos, stress and real Jaeger checks. Update guides, examples, specs and both report copies, commit and push.
- [ ] Recommend 20 optional follow-up features with their benefits, keeping them outside implementation until the user selects them.
