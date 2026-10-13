# Switching service-data to the 2.2 components

The 2.2 components were taken from `service-data` (`qsint/backend/service-data`). This plan
moves service-data onto them. It is applied by the service-data maintainers after the
importer work in progress there lands. The framework branch does not touch service-data.

**Goal: no difference on the wire.** Routes, bodies, status codes, headers, response bytes,
span names (`compat.<ns>.<op>`, attribute `compat.group`), metric names
(`sd_compat_responses_total`, `sd_http_*`) and log event names (`compat_request`,
`route_error`, `index_access_denied`, `dependency_unavailable`, `cache_unavailable`, …) stay
as they are. The framework classes take the service's logger, span prefix and observer for
exactly that reason.

## 1. Dependency

`pyproject.toml`: `qf-fastapi[tracing,operations]==2.2.0`, the `[tool.uv.sources]` path to
`dist/qf_fastapi-2.2.0-py3-none-any.whl`, then `uv lock && uv sync`. `jsonschema` stays a
direct dependency only if service-data imports it elsewhere (it does not today).

## 2. Per component

Strategy: keep the service-data module paths as thin modules (re-exports plus the service's
own configuration), so the ~150 importing files do not change. Tests that pin
service-specific behaviour stay in service-data; generic tests now live in the framework.

| service-data file | Becomes | Keeps (service-specific) |
|---|---|---|
| `common/observability/logging.py` | re-exports `EventLogger`, `redact`, `scrub`, `ServiceJSONFormatter = ScrubbingJSONFormatter`, `REDACTED_KEYS = DEFAULT_REDACTED_KEYS` from `framework.commons.events` | `ROOT_LOGGER = "service_data"`; `get_logger(name)` (prefixes `service_data.`, returns a framework `EventLogger`); `configure_logging(level) = configure_event_logging(ROOT_LOGGER, level)` |
| `common/observability/tracing.py` | re-exports `traced_span`, `traced`, `inject_http_headers`, `current_trace_id`, and `set_attributes = set_span_attributes` from `framework.tracing.spans` | `init_tracing` / `shutdown_tracing` / `tracer()` / `inject_headers` / `extract_context` (thin wrappers over `framework.tracing`, unchanged) |
| `common/resilience.py` | `from framework.resilience import BreakerState, CircuitBreaker` | the breaker table in the docstring |
| `common/registry/cache.py` | `class VersionedJsonCache(framework.redis.versioned_cache.VersionedJsonCache)` whose `__init__(redis: RedisStore, *, key_format=name_version_key, breaker=None, budget_s=CACHE_CALL_BUDGET_S, memo=False)` calls `super().__init__(redis.client, key=lambda n, v: key_format(redis, n, v), breaker=breaker or redis.breaker, budget_s=budget_s, memo=memo, observe=redis.observe, log=get_logger(__name__))` | `name_version_key`, `config_snapshot_key`, the key table; call sites unchanged |
| `entrypoints/startup.py` | `wait_for_dependencies` calls `framework.health.wait_for_dependencies(..., policy=GatePolicy(CHECK_TIMEOUT_S, BACKOFF_START_S, BACKOFF_CAP_S), log=log)`; re-exports `DependenciesUnavailable` | `dependency_checks`, `kafka_reachable` |
| `entrypoints/health.py` | `probe()` uses `run_probes(stores, timeout_s=PROBE_TIMEOUT_S)` and `readiness_status(checks, required=REQUIRED, started=started)`; `install_health_routes(app)` calls `install_readiness_routes(app, report)` | the store list, the `sd_dependency_up` gauge, module and runtime checks, `role` in the body |
| `common/http/encoding.py` | re-exports `dumps`, `LegacyJSONResponse` from `framework.operations.encoding` | — |
| `common/http/validation.py` | re-exports `ENUM_CI`, `VALIDATION_MESSAGE`, `ModelDefinitionError`, `ModelValidator`, `OBJECT_ONLY`, `compile_model`, `validation_body` | — |
| `common/http/identity.py` | re-exports `Identity`, `parse_roles`, `resolve_identity`, `with_user`, `requires_user`, `MISSING_USER`; `USER_HEADER = GATE_HEADERS.user`, `ROLES_HEADER = GATE_HEADERS.roles` | the QD-AUTH rule docstring |
| `common/http/profiles.py` | `QDataProfile = ErrorFieldProfile("error", "Invalid request: {path}")` instance, `QImporterProfile = ValidationBodyProfile()` instance; `PROFILES = MappingProxyType({"qdata": ..., "api": ...})`; `PROFILE_REGISTRY = ProfileRegistry(PROFILES, default=None)`; `profile_for = PROFILE_REGISTRY.for_prefix` | the two families |
| `common/http/catalogue.py` | re-exports `BODY_KINDS`, `BodyKind`, `CatalogueError`, `EndpointMap`, `OperationSpec`, `RouteCatalogue`, `describe_routes`, `load_map` | `catalogue(maps) = RouteCatalogue.from_maps(maps, PROFILE_REGISTRY)` for the CLI |
| `common/http/chain.py` | re-exports `OperationCall`, `ChainState`, `ChainLink`, `Handler`, the link classes, `DEFAULT_LINKS`, `OperationChain`, `respond`; `BAD_REQUEST` / `UNSUPPORTED_MEDIA` / `INTERNAL_ERROR` / `is_json_mimetype` from `framework.operations.werkzeug_answers` | `NO_INDEX_FOR_PROFILE`; `ServiceObserver(OperationObserver)` (increments `ctx.metrics.operation_responses`, logs `compat_request` / `compat_request_failed` with the service logger); `service_chain()` (below) |
| `common/http/errors.py` | `install_exception_handlers(app)` calls the framework's with `log=log`; re-exports `WERKZEUG_404 = NOT_FOUND_HTML`, `METHOD_NOT_ALLOWED`, `not_found_response`, the handlers | — |
| `common/http/middleware.py` | re-exports `TrailingSlashMiddleware`, `BodySizeLimitMiddleware`, `SecurityHeadersMiddleware`, `CorsMiddleware = EchoOriginCorsMiddleware`, `TOO_LARGE`, `SECURITY_HEADERS` | `HttpMetricsMiddleware` (service metric names and group labels) |
| `common/http/operations.py` | re-exports `api_operation`, `APP_STATE_*`; `mount_endpoint_maps(app, maps, chain=None)` calls the framework's with `chain or service_chain()` and `PROFILE_REGISTRY` | — |
| `common/errors.py` | `class HttpError(OperationError, ServiceError)` (same constructor); `class ClientInputError(framework ClientInputError, ServiceError)` | every other domain exception, and the `NotFound`/`Forbidden`/... defaults |
| `entrypoints/query.py` | the mount + exception handlers + five `add_middleware` lines become one `install_operations(app, maps, chain=service_chain(), profiles=PROFILE_REGISTRY, max_body_bytes=settings.http_max_body_bytes, cors_origins=settings.cors_allowed_origins, extra_middleware=[(HttpMetricsMiddleware, {"owner": app})], log=log)` | the rest of the lifespan |
| `tests/support/apps.py` | `operations_app` uses `install_operations` the same way | `fake_context`, `running_query_app` |
| `tests/support/tracing.py` | the `spans` fixture wraps `framework.testing.spans.capture_spans()`; `CapturedSpans` re-exported | — |

`service_chain()` builds the chain exactly as today:

```python
def service_chain() -> OperationChain:
    mapper = ErrorMapper(log=log).register(IndexAccessDenied, index_access_denied)
    links = default_links(super_users=lambda ctx: ctx.settings.super_users, span_prefix="compat")
    return OperationChain(links, mapper, ServiceObserver(log))


def index_access_denied(exc, spec):
    log.warning("index_access_denied", route=spec.path, roles=exc.roles)
    return {"status": "FORBIDDEN", "error": NO_INDEX_FOR_PROFILE, "roles": exc.roles}, 403
```

## 3. Differences to expect (none on the wire)

| Where | Difference | Impact |
|---|---|---|
| Start-up errors of a broken endpoint map | `CatalogueError` texts: "is not an ApiProfile" (was "a"); an unknown handler names the map path | Messages only; nothing is served in that case |
| `CircuitBreaker(threshold < 1 or open_s <= 0)` | Raises `ValueError` | service-data uses (3, 30) and (5, 30) |
| `ChainState` | No `raw` / `result` fields (never read) | None |
| `ScrubbingJSONFormatter` handler marker | `_qf_events` instead of `_sd` | None (idempotence check only) |
| Tests moved to the framework | Generic contract tests (`tests/unit/common/http/test_operations.py`, the cache/breaker parts of `test_kernel.py`, `test_startup_gate.py`) can shrink to the service-specific cases (QData profile body, index-access 403, `compat.*` span names, `sd_*` metrics) | Fewer duplicated tests |

## 4. Verification in service-data

`ruff`, `mypy src`, `interrogate`, `lint-imports`, `pytest tests/unit tests/contract`, the
integration and e2e suites, and the read parity audit against QData
(`tools/parity/run.py --suite read`): 0 unexplained, identical counts to before the switch.
