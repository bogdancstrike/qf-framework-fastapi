# Changelog

## 2.1.0 — 2026-10-13

Opt-in options for batch-importer pipelines (QImporter parity). Workers that do not
use them behave exactly as in 2.0. Analysis and rationale: [docs/ImporterParity.md](docs/ImporterParity.md).

### Added
- `@kafka_handler(split_arrays=True)`: a JSON-array value carries one logical message per object element; `[]` is a skip, non-object elements are dropped (`qf_decode_dropped_total`), invalid JSON and non-objects are dead-lettered at stage `decode` with the record bytes.
- `@kafka_handler(with_context=True)`: handlers receive `context=` (`RecordContext`: coordinates, key, record timestamp and type, headers, element index; a list for bulk handlers).
- Per-message results: handlers may return `Outcome.ok/skip/dead_letter/retry` (one per message for bulk handlers). Outputs refused permanently by the broker dead-letter their message with stage `forward` and header `target_topic`.
- `DeadLetterFormat` strategy for dead letters: `DiagnosticEnvelope` (2.0 format, default) and `HeaderDeadLetter` (replayable value plus `reason`, `consumer`, `failed_at`, `stage`, worker and `source_*` headers).
- `@retry_to_dlq(dlq_format=, transient=, transient_backoff=, backoff=)`; `Backoff` strategies `FullJitter` (2.0 default) and `ExponentialBackoff`.
- Per-worker `consumer_group`, `bootstrap_servers` and `auto_offset_reset`: one consumer per distinct triple; outputs and dead letters use a producer of the worker's cluster. `KafkaRuntime(consumer_factory=, producer_factory=)` for tests.
- Per-worker `inject_id`, `key_by` and `encoder` (`compact_json`, `python_json`); `topics_out=[]` declares a sink.
- Settings `disabled_workers`, `verify_topics` (`MissingTopicsError`), `producer_max_request_size`, `producer_request_timeout_ms`.
- `KafkaClient.put_message(..., headers=, timestamp_ms=)`.
- Workers declared on methods (or with deferred values `(resolver) -> value`) are collected per instance with `collect_workers(owner, resolver, override=)`; module-level functions still register at import.
- Worker options `raw=True` (undecoded bytes) and `enabled=` (declared but not started).
- A topic may be read by several workers whose consumer groups differ.
- `KafkaSupervisor`: one runtime per consumer lane, restarted alone with backoff; per-worker `status()`.
- `WorkerObserver` hooks (`on_job` with outcome labels, `on_transient`, `on_recovered`, `on_lag`, `on_state`, `on_restart`).
- `trace_kafka_operations` setting: `kafka.poll` and `kafka.publish` spans.
- `framework.streams.admin.KafkaAdmin`: committed/end offsets, lag, offsets for a time, group membership, commit offsets without joining.
- Metrics `qf_decode_dropped_total{worker,reason}`, `qf_dlq_truncated_total{worker}`; `qf_retries_total{kind="transient"}`; `Outcome(label=...)` values in `qf_messages_total{outcome}`.

### Changed
- A dead letter the broker refuses permanently (for example too large) is replaced by `{"_truncated": true, "size", "sha256"}` with the same headers instead of exhausting the transport budget and failing the runtime.
- Workers may declare `topics_out=[]`.

## 2.0.0 — 2026-10-11

FastAPI and asyncio successor of QF Flask. See [README](README.md) and [docs](docs/README.md).
