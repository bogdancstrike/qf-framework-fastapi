import asyncio

import fakeredis.aioredis
import pytest
from framework.decorators import (
    call_retry,
    call_circuit_breaker,
    call_rate_limit,
    CircuitOpenError,
    RateLimitExceededError,
)
from framework.decorators.single_execute import SingleExecute


async def test_async_retry_awaits_and_retries():
    calls = 0

    @call_retry(max_attempts=3, wait_fixed=0)
    async def work():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ValueError()
        return 3

    assert await work() == 3


async def test_async_circuit_tracks_awaited_failure():
    @call_circuit_breaker(fail_max=1)
    async def fail():
        raise ValueError()

    with pytest.raises(ValueError):
        await fail()
    with pytest.raises(CircuitOpenError):
        await fail()


async def test_async_rate_limit():
    @call_rate_limit(per_second=1)
    async def work():
        return 1

    assert await work() == 1
    with pytest.raises(RateLimitExceededError):
        await work()


async def test_single_execute_async_excludes_concurrency_and_zero_error_cooldown():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    running, release = asyncio.Event(), asyncio.Event()

    @SingleExecute(lock_id="job", redis_client=redis, cooldown_sec=30, cooldown_on_error_sec=0)
    async def work():
        running.set()
        await release.wait()
        raise ValueError()

    first = asyncio.create_task(work())
    await running.wait()
    assert await work() is None
    release.set()
    with pytest.raises(ValueError):
        await first
    assert not await redis.exists("singleexec:job")
    await redis.aclose()
