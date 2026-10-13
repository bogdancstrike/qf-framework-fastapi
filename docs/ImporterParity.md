# Running a QImporter-style ingestion service on QF 2.1

QImporter (`service-data-importer`) consumes source topics in batches, routes each
message to a handler by its `type`, writes documents to Elasticsearch, forwards them
to the enrichment chain and dead-letters failures with context headers. Its
replacement runs every pipeline as a `@kafka_handler` worker declared in code
(worker names and topics in the decorator, not in a database row). This page lists
what that needs from the framework, what 2.1 adds for it, and what stays in the
application.

Rule ids (`FLOW-03`, `GAP-ENG-10` …) refer to the QImporter business specification
(`docs/BUSINESS.md` of `service-data-importer`).

## What 2.1 adds, and why

| QImporter behaviour | Problem it creates / requirement | 2.1 option | Default |
|---|---|---|---|
| A record value may be a JSON **array**; each object element is one message, other elements are skipped (FLOW-03) | 2.0 dead-lettered arrays before user code | `split_arrays=True`: one logical message per object element (`element_index`), `[]` is a skip, non-object elements dropped and counted, invalid JSON or non-objects dead-lettered at stage `decode` with the record bytes unchanged (QImporter only logged and dropped them) | off |
| The stale-version rule compares a document's source time with the **record timestamp**; deterministic ids use `(topic, partition, offset, element)` | 2.0 handlers saw only the decoded value | `with_context=True`: keyword `context` = `RecordContext` (topic, partition, offset, key, `timestamp_ms`, `timestamp_type`, headers, `element_index`, group); a list for bulk handlers | off |
| A batch is processed by one call; each message has its own result, failures go to the dead-letter topic one by one (FLOW-04, RTE-05) | 2.0 bulk handlers succeed or fail as a whole: one bad item dead-letters or retries its 499 neighbours | Return `Outcome` objects: `ok(*docs)`, `skip(reason)`, `dead_letter(reason, stage, values, headers)`, `retry(reason)` per message; the record commits when all its messages are terminal | whole-job result |
| Dead letters carry the failed value itself plus headers `reason`, `consumer`, `failed_at` (FLOW-06); the replacement adds source coordinates so they can be replayed | 2.0 published a diagnostic envelope (base64 value, no reason text) | `DeadLetterFormat` strategy; `HeaderDeadLetter(worker_header="flow", reason_limit=2000, max_value_bytes=…)` | `DiagnosticEnvelope` |
| A dead letter larger than the broker limit was lost (GAP-ENG-08) | A source must never be committed without its dead letter | Any dead letter the broker refuses permanently is replaced by `{"_truncated": true, "size", "sha256"}` with the same headers; counted in `qf_dlq_truncated_total` | always (all formats) |
| A forward that fails dead-letters the result (FLOW-07) | With per-message outcomes, one oversized document must not fail the runtime | In outcome mode a permanent producer refusal (size, invalid/unauthorised topic) dead-letters that message with stage `forward` and header `target_topic`; transient errors keep the transport retry budget | outcome mode only |
| An Elasticsearch/Redis outage dead-lettered whole batches; offsets were auto-committed anyway (FLOW-05, DEF-02) | An outage is not a message defect: wait, keep the offsets, never dead-letter | `@retry_to_dlq(transient=(StoreDown,), transient_backoff=…)`: unlimited retries of the same messages with `ExponentialBackoff` (1, 2, 4 … 60 s, ±20 %), no commit meanwhile; revocation or shutdown stops it | none |
| Predictable retry schedule | 2.0 used full jitter only | `@retry_to_dlq(backoff=ExponentialBackoff(...))` | full jitter |
| One consumer group for every flow, so one flow's rebalance stalls all of them (FLOW-02, GAP-ENG-11); the STT flows read **another cluster** | Per-flow isolation and cut-over offset migration need a group per flow | `consumer_group=`, `bootstrap_servers=`, `auto_offset_reset=` per worker: one consumer ("lane") per distinct triple, outputs and dead letters on the worker's own cluster | one shared consumer |
| Messages are never reshaped: no added keys (RTE-03, DEF-03) | 2.0 injected `id` into messages without one | `inject_id=False` | inject |
| Forwards keyed by the document id (GAP-ENG-09: QImporter sent no key, so versions raced) | Key outputs by `system._id` | `key_by="system._id"` (`None`: the source record key) | `"id"` |
| Producers serialise with `json.dumps(value)` defaults | Byte-identical forwards for downstream workers | `encoder=python_json` (`compact_json` is the 2.0 format) | compact |
| The STT relay publishes nothing | 2.0 required an output topic | `topics_out=[]` declares a sink; returning outputs from a sink is an application error | — |
| `DISABLE_KAFKA` and per-flow switches | Turn workers off per environment | `QSINT_DISABLED_WORKERS=["name", …]` (unknown names refuse start-up) | all on |
| A misspelt topic was auto-created by the producer (GAP-ENG-14) | Refuse to run against missing topics | `QSINT_VERIFY_TOPICS=true`: start-up checks every input, output and dead-letter topic on each cluster and raises `MissingTopicsError` without creating anything | off |
| A dead-letter replay must keep the original record timestamp | `put_message` could not set one | `KafkaClient.put_message(..., headers=..., timestamp_ms=...)` | — |
| Producer limits | Size the request limit explicitly | `QSINT_PRODUCER_MAX_REQUEST_SIZE`, `QSINT_PRODUCER_REQUEST_TIMEOUT_MS` | aiokafka defaults |

### Already in 2.0 (no change needed)

| QImporter behaviour or gap | 2.0 mechanism |
|---|---|
| A batch longer than `max_poll_interval_ms` was processed twice (GAP-ENG-10) | Polling never blocks on handlers: jobs run as tasks while the loop keeps polling, so a slow batch never evicts the member |
| Forwarding and dead-lettering were fire-and-forget (GAP-ENG-08) | `AckPublisher` waits for every broker acknowledgement before the source completes |
| Auto-commit committed failed batches (FLOW-05) | ACK-gated contiguous commits (`CommitCoordinator`) |
| No graceful shutdown (GAP-ENG-15) | Bounded drain on stop and on revocation, commit of the completed prefix |
| Trace context lost across Kafka | W3C headers injected on every publication, extracted per record |
| Idempotent, `acks=all` producers | Default producer configuration |

## What stays in the application

These are business rules, not runtime mechanics; the importer module implements them
with the options above:

- routing by `type` (top-level `type`, then the topic's mapping, then the topic-name
  derivation, per record) and restoring `type` in dead-letter values
  (`Outcome.dead_letter(values=[...])`);
- the document pipelines, deduplication and the stale-version rule (using
  `context.timestamp_ms`);
- counting business outcomes per type (`Outcome(label=...)` already feeds
  `qf_messages_total{outcome}`; type-level metrics belong to the application);
- offset migration from the legacy group and the dead-letter replay command (built on
  `AIOKafkaAdminClient` and `KafkaClient.put_message(timestamp_ms=...)`).

## Compatibility

Nothing changes for a worker that does not use the new options: decoding, `id`
injection, output encoding and keys, the diagnostic envelope, retries, commits and
the single shared consumer behave exactly as in 2.0, and the 2.0 test suite passes
unchanged. Two things are new for every worker: a dead letter the broker refuses
as too large is replaced by a reference instead of failing the runtime, and
`topics_out=[]` is accepted.
