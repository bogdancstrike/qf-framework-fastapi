import pytest
from framework.config import FrameworkSettings, load_framework_settings


def test_http_only_no_services():
    assert not FrameworkSettings(enable_etl=False).enable_etl


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"poll_max_records": 0},
        {"pending_max_per_tp": 0},
        {"api_port": 65536},
        {"retry_attempts": 0},
        {"retry_base_sec": 20},
        {"enable_tracing": True},
        {"kafka_security_protocol": "bad"},
        {"kafka_security_protocol": "SASL_SSL"},
    ],
)
def test_validation(kwargs):
    with pytest.raises(ValueError):
        FrameworkSettings(**kwargs)


def test_legacy_config_and_environment(monkeypatch):
    class Config:
        WORKER_NAME = "legacy"
        KAFKA_BOOTSTRAP_SERVERS = "host:9092"
        WORKER_MODULE = "workers.workers"
        REDIS_HOST = "redis"
        REDIS_PORT = 6380
        REDIS_DB = 2

    monkeypatch.setenv("WORKER_NAME", "environment")
    loaded = load_framework_settings(config=Config, overrides={"poll_max_records": 42})
    assert loaded.consumer_name == "environment"
    assert loaded.worker_modules == ["workers.workers"]
    assert loaded.redis_url.get_secret_value() == "redis://redis:6380/2"
    assert loaded.poll_max_records == 42
    assert (
        load_framework_settings(
            config=Config, overrides={"consumer_name": "explicit"}
        ).consumer_name
        == "explicit"
    )


def test_secrets_redacted(settings):
    settings.redis_url = "redis://:secret@localhost/0"
    # Validated construction keeps credentials out of repr.
    secret = FrameworkSettings(enable_etl=False, redis_url="redis://:secret@localhost/0")
    assert "secret" not in repr(secret)
