# Configuration reference

Generated from `FrameworkSettings` fields. Use `QSINT_` + uppercase field name unless an alias is listed. Explicit values override environment, then dotenv; the Config bridge adds fallback defaults. Lists and dictionaries in environment variables use JSON. Secret defaults are displayed only when safe.

| Field | Default | Additional aliases / legacy keys |
|---|---|---|
| `enable_etl` | `True` |  |
| `enable_api` | `True` |  |
| `enable_dynamic_endpoints` | `True` |  |
| `api_host` | `0.0.0.0` |  |
| `api_port` | `4000` |  |
| `api_version` | `2.0` |  |
| `api_title` | `QF Worker API` |  |
| `api_description` | `Kafka workers and HTTP API` |  |
| `endpoint_json_path` | `maps/endpoint.json` | `QSINT_ENDPOINT_JSON`, `QSINT_ENDPOINT_JSON_PATH`, `QSINT_ENDPOINT_JSON (Config bridge)` |
| `worker_modules` | `[]` | `WORKER_MODULES (Config bridge)` |
| `kafka_bootstrap_servers` | `None` | `KAFKA_BOOTSTRAP_SERVERS`, `QSINT_KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_BOOTSTRAP_SERVERS (Config bridge)` |
| `consumer_name` | `None` | `WORKER_NAME`, `QSINT_CONSUMER_NAME`, `WORKER_NAME (Config bridge)` |
| `error_topic` | `qf.errors` | `ERROR_TOPIC (Config bridge)` |
| `fetch_max_wait_ms` | `50` |  |
| `poll_timeout_ms` | `25` | `KAFKA_POLL_TIMEOUT_MS (Config bridge)` |
| `poll_max_records` | `500` | `KAFKA_POLL_MAX_RECORDS (Config bridge)` |
| `pending_max_per_tp` | `750` | `KAFKA_PENDING_MAX_PER_TP (Config bridge)` |
| `commit_tick_sec` | `0.2` | `KAFKA_COMMIT_TICK_SEC (Config bridge)` |
| `commit_timeout_sec` | `10` |  |
| `max_poll_interval_ms` | `300000` |  |
| `rebalance_timeout_sec` | `30` |  |
| `shutdown_timeout_sec` | `30` |  |
| `startup_timeout_sec` | `60` |  |
| `connect_retry_base_sec` | `0.5` | `KAFKA_CONNECT_RETRY_SLEEP_SEC (Config bridge)` |
| `connect_attempts` | `5` | `KAFKA_CONNECT_RETRIES (Config bridge)` |
| `retry_attempts` | `3` |  |
| `retry_base_sec` | `0.25` |  |
| `retry_max_sec` | `10` |  |
| `transport_attempts` | `5` |  |
| `output_ack_timeout_sec` | `30` | `KAFKA_OUTPUT_ACK_TIMEOUT_SEC (Config bridge)` |
| `producer_linger_ms` | `5` | `KAFKA_PRODUCER_LINGER_MS (Config bridge)` |
| `producer_batch_size` | `65536` | `KAFKA_PRODUCER_BATCH_SIZE (Config bridge)` |
| `kafka_security_protocol` | `PLAINTEXT` |  |
| `kafka_sasl_mechanism` | `PLAIN` |  |
| `kafka_sasl_username` | `None` |  |
| `kafka_sasl_password` | `None` |  |
| `kafka_ssl_cafile` | `None` |  |
| `redis_url` | `redis://localhost:6379/0` |  |
| `idempotency_enabled` | `False` |  |
| `idempotency_ttl_sec` | `86400` |  |
| `redis_max_connections` | `100` | `REDIS_MAX_CONNECTIONS (Config bridge)` |
| `redis_timeout_sec` | `5` | `REDIS_SOCKET_TIMEOUT (Config bridge)` |
| `aggregate_lease_sec` | `30` |  |
| `enable_tracing` | `False` | `QSINT_ENABLE_TRACING`, `ENABLE_TRACING`, `ENABLE_TRACING (Config bridge)` |
| `otlp_endpoint` | `None` | `QSINT_OTLP_ENDPOINT (Config bridge)` |
| `otlp_insecure` | `False` |  |
| `service_name` | `None` |  |
| `log_endpoints` | `False` | `LOG_ENDPOINTS`, `QSINT_LOG_ENDPOINTS`, `LOG_ENDPOINTS (Config bridge)` |
| `log_level` | `INFO` |  |
| `cors_origins` | `[]` |  |
| `init_app` | `None` |  |
| `extra` | `{}` |  |

Settings validation requires ETL bootstrap/group/worker modules when ETL is enabled; a tracing endpoint when tracing is enabled; complete SASL credentials for SASL protocols; rebalance timeout below max poll interval; and retry base no greater than retry maximum. Individual fields also enforce positive/range constraints. See [Operations.md](Operations.md) for tuning and delivery implications.

Legacy `WORKER_MODULE` becomes a one-element worker list. Redis host/port/db/password fields form a URL. `QSINT_` fields on Config are also supported. Unknown extra environment keys are ignored; unsupported unsafe delivery knobs do not disable ACK-gated commits.
