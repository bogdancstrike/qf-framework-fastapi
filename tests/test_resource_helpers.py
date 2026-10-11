import json

import pytest
from redis.exceptions import TimeoutError as RedisTimeoutError

from framework.redis.redis_utils import RedisUtils
from framework.streams.kafka_client import KafkaClient


async def test_redis_context_closes_owned_pool_after_failed_ping(monkeypatch):
    helper = RedisUtils()
    closed = []

    async def fail():
        raise RuntimeError("unavailable")

    async def close():
        closed.append(True)

    monkeypatch.setattr(helper.redis, "ping", fail)
    monkeypatch.setattr(helper.redis, "aclose", close)
    with pytest.raises(RuntimeError):
        await helper.__aenter__()
    assert closed == [True]


async def test_redis_timeout_retry_option_is_applied():
    helper = RedisUtils(retry_on_timeout=True)
    try:
        assert RedisTimeoutError in helper.redis.connection_pool.connection_kwargs["retry_on_error"]
    finally:
        await helper.close()


async def test_kafka_context_clears_closed_producer_and_rejects_double_enter(monkeypatch):
    class Producer:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def start(self):
            pass

        async def stop(self):
            pass

        async def send_and_wait(self, topic, value, **kwargs):
            return json.loads(value)

    monkeypatch.setattr("framework.streams.kafka_client.AIOKafkaProducer", Producer)
    helper = KafkaClient()
    async with helper:
        with pytest.raises(RuntimeError, match="already"):
            await helper.__aenter__()
        with pytest.raises(ValueError):
            await helper.put_message("in", {"value": float("nan")})
    assert helper.producer is None
    with pytest.raises(RuntimeError, match="async with"):
        await helper.put_message("in", {})
    async with helper:
        assert await helper.put_message("in", {"id": "fresh"}) == {"id": "fresh"}
