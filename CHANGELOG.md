# Changelog

## 2.2.2 — 2026-10-14

Observability bounds needed by service-data's T-02 scenarios.

### Added
- `init_tracing(resource_attributes=…, export_timeout_s=…)`: extra tracing resource attributes next to `service.name`, and an upper bound for every export, retries included (default 3 s instead of the exporter's 10 s). `FrameworkSettings.tracing_resource_attributes` and `otlp_export_timeout_sec` feed the application runner.
- `ScrubbingJSONFormatter(max_line_length=200)` and `configure_event_logging(max_traceback_line=…)`: traceback lines longer than the limit are cut to `<first chars>… <truncated len=N>` (an exception message can carry a whole document); `truncate_lines` helper.

### Changed
- `shutdown_tracing()` closes the provider (export what is queued, then shut down) instead of only flushing; with an unreachable collector it returns within about the export timeout. The framework tracer is a no-op afterwards.

### Fixed
- `scrub` hides the password of a DSN without a user (`redis://:pw@host` → `redis://:***@host`).

## 2.2.1 — 2026-10-13

Found while switching service-data to 2.2.0.

### Added
- `ApiProfile.non_object_body(body)`: a profile can answer a JSON body that is not an object (e.g. Werkzeug's 400 for `null`); the default keeps the flask-restx validation body.
- `py.typed`: the package ships its type hints, so applications type-check against the framework (subclasses of framework classes are no longer `Any` to mypy).

### Changed
- `VersionedJsonCache` is no longer generic at class level: `get_or_build` is typed per call, since one cache serves names of different types.

## 2.2.0 — 2026-10-13

Reusable application components, taken from a QSINT service (service-data) so other services
stop re-implementing them. All are opt-in imports: applications that do not use them behave
exactly as in 2.1. Guides: [docs/Operations-HTTP.md](docs/Operations-HTTP.md),
[docs/ReusableComponents.md](docs/ReusableComponents.md).

### Added
- `framework.operations` (extra `operations`, adds `jsonschema`): endpoint-map routes served with the Flask / flask-restx / Werkzeug wire contract. `@api_operation`, `OperationCall`, `OperationChain` (links `MediaTypeGuard`, `JsonDecoder`, `BodyKindPolicy`, `SchemaValidator`, `IdentityResolver`, `HandlerInvoker`), per-endpoint `operation_contract` (`model`, `body_kind`) and `operation_schemas` (Draft 4, strict types, nested models, `enum_ci`), `RouteCatalogue` / `load_map` / `describe_routes`, `ApiProfile` strategies (`ValidationBodyProfile`, `ErrorFieldProfile`) in a `ProfileRegistry`, `OperationError` / `ClientInputError` and an `ErrorMapper` applications extend with `register`, `OperationObserver`, `LegacyJSONResponse`, legacy exception handlers (HTML 404, JSON 405, OPTIONS 200), `BodySizeLimitMiddleware`, `SecurityHeadersMiddleware`, `EchoOriginCorsMiddleware`, `TrailingSlashMiddleware`, and `install_operations` to set all of it up.
- Gateway identity in `framework.operations`: `Identity`, `GateHeaders` (default `X-User-Name` / `X-User-Roles`), `resolve_identity`, `@with_user` and `user_required(body, status)`, `requires_user`.
- `framework.health`: `wait_for_dependencies` (start-up gate with per-check timeout, backoff and deadline, `DependenciesUnavailable`), `run_probes`, `readiness_status` (`ready` / `degraded` / `not_ready`), `install_readiness_routes` (`/health/ready`, `/health/dependencies`).
- `framework.commons.events`: `EventLogger` (`log.info("event", key=value)`), `redact`, `scrub`, `ScrubbingJSONFormatter`, `get_event_logger`, `configure_event_logging`.
- `framework.tracing.spans`: `traced_span`, `@traced`, `set_span_attributes`, `span_attributes`, `inject_http_headers`, `current_trace_id` (attributes redacted like log fields).
- `framework.resilience.CircuitBreaker`: explicit allow/success/failure breaker for callers that degrade instead of raising.
- `framework.redis.versioned_cache.VersionedJsonCache`: Redis read-through JSON cache keyed by name and data version, with call budget, breaker, observer and in-process memo.
- `framework.testing.spans.capture_spans`: in-memory span capture for tests.

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
