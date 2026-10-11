from pathlib import Path
from typing import Any, Callable

from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class FrameworkSettings(BaseSettings):
    """Validated settings. Explicit arguments override env; env overrides dotenv."""

    model_config = SettingsConfigDict(
        env_prefix="QSINT_",
        env_file=".env",
        extra="ignore",
        populate_by_name=True,
    )
    enable_etl: bool = True
    enable_api: bool = True
    enable_dynamic_endpoints: bool = True
    api_host: str = "0.0.0.0"
    api_port: int = Field(default=4000, ge=1, le=65535)
    api_version: str = "2.0"
    api_title: str = "QF Worker API"
    api_description: str = "Kafka workers and HTTP API"
    endpoint_json_path: str = Field(
        default="maps/endpoint.json",
        validation_alias=AliasChoices("QSINT_ENDPOINT_JSON", "QSINT_ENDPOINT_JSON_PATH"),
    )
    worker_modules: list[str] = Field(default_factory=list)
    kafka_bootstrap_servers: str | None = Field(
        default=None,
        validation_alias=AliasChoices("KAFKA_BOOTSTRAP_SERVERS", "QSINT_KAFKA_BOOTSTRAP_SERVERS"),
    )
    consumer_name: str | None = Field(
        default=None, validation_alias=AliasChoices("WORKER_NAME", "QSINT_CONSUMER_NAME")
    )
    error_topic: str = Field(default="qf.errors", min_length=1)
    poll_timeout_ms: int = Field(default=25, ge=1)
    poll_max_records: int = Field(default=500, ge=1)
    pending_max_per_tp: int = Field(default=750, ge=1)
    commit_tick_sec: float = Field(default=0.2, gt=0)
    commit_timeout_sec: float = Field(default=10, gt=0)
    max_poll_interval_ms: int = Field(default=300000, ge=1000)
    rebalance_timeout_sec: float = Field(default=30, gt=0)
    shutdown_timeout_sec: float = Field(default=30, gt=0)
    startup_timeout_sec: float = Field(default=60, gt=0)
    connect_retry_base_sec: float = Field(default=0.5, ge=0)
    connect_attempts: int = Field(default=5, ge=1)
    retry_attempts: int = Field(default=3, ge=1)
    retry_base_sec: float = Field(default=0.25, ge=0)
    retry_max_sec: float = Field(default=10, gt=0)
    transport_attempts: int = Field(default=5, ge=1)
    output_ack_timeout_sec: float = Field(default=30, gt=0)
    producer_linger_ms: int = Field(default=5, ge=0)
    producer_batch_size: int = Field(default=65536, ge=1)
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str | None = None
    kafka_sasl_password: SecretStr | None = None
    kafka_ssl_cafile: Path | None = None
    redis_url: SecretStr = SecretStr("redis://localhost:6379/0")
    idempotency_enabled: bool = False
    idempotency_ttl_sec: int = Field(default=86400, ge=1)
    redis_max_connections: int = Field(default=100, ge=1)
    redis_timeout_sec: float = Field(default=5, gt=0)
    aggregate_lease_sec: int = Field(default=30, ge=3)
    enable_tracing: bool = Field(
        default=False, validation_alias=AliasChoices("QSINT_ENABLE_TRACING", "ENABLE_TRACING")
    )
    otlp_endpoint: str | None = None
    otlp_insecure: bool = False
    service_name: str | None = None
    log_endpoints: bool = Field(
        default=False, validation_alias=AliasChoices("LOG_ENDPOINTS", "QSINT_LOG_ENDPOINTS")
    )
    log_level: str = "INFO"
    cors_origins: list[str] = Field(default_factory=list)
    init_app: Callable[[Any], None] | None = Field(default=None, exclude=True)
    extra: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_config(cls, config, **overrides):
        return load_framework_settings(config=config, overrides=overrides)

    @model_validator(mode="after")
    def validate_runtime(self):
        if self.enable_etl and (
            not self.kafka_bootstrap_servers or not self.consumer_name or not self.worker_modules
        ):
            raise ValueError(
                "ETL requires worker_modules, kafka_bootstrap_servers and consumer_name"
            )
        if self.kafka_security_protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
            raise ValueError("Invalid Kafka security protocol")
        if self.kafka_security_protocol.startswith("SASL") and not (
            self.kafka_sasl_username and self.kafka_sasl_password
        ):
            raise ValueError("SASL requires username and password")
        if self.rebalance_timeout_sec * 1000 >= self.max_poll_interval_ms:
            raise ValueError("rebalance_timeout_sec must be below max_poll_interval_ms")
        if self.retry_base_sec > self.retry_max_sec:
            raise ValueError("retry_base_sec must not exceed retry_max_sec")
        if self.enable_tracing and not self.otlp_endpoint:
            raise ValueError("Tracing requires otlp_endpoint")
        return self


LEGACY_KEYS = {
    "WORKER_NAME": "consumer_name",
    "KAFKA_BOOTSTRAP_SERVERS": "kafka_bootstrap_servers",
    "WORKER_MODULES": "worker_modules",
    "ERROR_TOPIC": "error_topic",
    "KAFKA_POLL_TIMEOUT_MS": "poll_timeout_ms",
    "KAFKA_POLL_MAX_RECORDS": "poll_max_records",
    "KAFKA_PENDING_MAX_PER_TP": "pending_max_per_tp",
    "KAFKA_COMMIT_TICK_SEC": "commit_tick_sec",
    "KAFKA_CONNECT_RETRIES": "connect_attempts",
    "KAFKA_CONNECT_RETRY_SLEEP_SEC": "connect_retry_base_sec",
    "KAFKA_OUTPUT_ACK_TIMEOUT_SEC": "output_ack_timeout_sec",
    "KAFKA_PRODUCER_LINGER_MS": "producer_linger_ms",
    "KAFKA_PRODUCER_BATCH_SIZE": "producer_batch_size",
    "REDIS_MAX_CONNECTIONS": "redis_max_connections",
    "REDIS_SOCKET_TIMEOUT": "redis_timeout_sec",
    "ENABLE_TRACING": "enable_tracing",
    "QSINT_OTLP_ENDPOINT": "otlp_endpoint",
    "QSINT_ENDPOINT_JSON": "endpoint_json_path",
    "LOG_ENDPOINTS": "log_endpoints",
}


def load_framework_settings(*, dotenv_path=None, overrides=None, config=None):
    """Read existing config.Config optionally; explicit overrides have highest precedence.

    Config values are fallback defaults. Environment/dotenv values override them.
    """
    import importlib
    import os
    from dotenv import dotenv_values

    if config is None:
        try:
            module = importlib.import_module("config")
        except ModuleNotFoundError as exc:
            if exc.name != "config":
                raise
        else:
            config = getattr(module, "Config", None)
    defaults = {}
    if config is not None:
        for old, new in LEGACY_KEYS.items():
            if hasattr(config, old):
                defaults[new] = getattr(config, old)
        if "worker_modules" not in defaults and hasattr(config, "WORKER_MODULE"):
            defaults["worker_modules"] = [config.WORKER_MODULE]
        from urllib.parse import quote

        if hasattr(config, "REDIS_HOST"):
            password = getattr(config, "REDIS_PASSWORD", None)
            auth = ":" + quote(str(password), safe="") + "@" if password else ""
            defaults["redis_url"] = (
                f"redis://{auth}{config.REDIS_HOST}:{getattr(config, 'REDIS_PORT', 6379)}/{getattr(config, 'REDIS_DB', 0)}"
            )
        for field in FrameworkSettings.model_fields:
            old = "QSINT_" + field.upper()
            if hasattr(config, old):
                defaults[field] = getattr(config, old)
    env = {**dotenv_values(dotenv_path or ".env"), **os.environ}
    for field, info in FrameworkSettings.model_fields.items():
        aliases = ["QSINT_" + field.upper()]
        if isinstance(info.validation_alias, AliasChoices):
            aliases.extend(info.validation_alias.choices)
        aliases.extend(old for old, new in LEGACY_KEYS.items() if new == field)
        for alias in aliases:
            if alias in env:
                value = env[alias]
                if field in {"worker_modules", "cors_origins", "extra"}:
                    import json

                    value = json.loads(value)
                defaults[field] = value
                break
    return FrameworkSettings(_env_file=dotenv_path or ".env", **{**defaults, **(overrides or {})})
