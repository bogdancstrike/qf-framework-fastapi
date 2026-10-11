import asyncio

import fakeredis
import fakeredis.aioredis
import pytest

from framework.commons.apdu_utils import apdu_compact, apdu_expand, lower_rx, nth_obj
from framework.commons.hex import loi, s2b
from framework.decorators import call_circuit_breaker, CircuitOpenError, kafka_handler
from framework.decorators.kafka_workers import all_workers
from framework.decorators.single_execute import SingleExecute


async def test_single_execute_renews_fractional_lease_during_longer_work():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    entered = asyncio.Event()
    try:

        @SingleExecute(
            lock_id="renew",
            redis_client=client,
            run_ttl_sec=0.2,
            renew_every_sec=0.04,
            cooldown_sec=0,
        )
        async def work():
            entered.set()
            await asyncio.sleep(0.35)
            return "done"

        first = asyncio.create_task(work())
        await entered.wait()
        await asyncio.sleep(0.25)
        assert await work() is None
        assert await first == "done"
        assert not await client.exists("singleexec:renew")
    finally:
        await client.aclose()


async def test_single_execute_cleanup_failure_preserves_function_error(monkeypatch):
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:

        async def fail_cleanup(*args):
            raise ConnectionError("cleanup failed")

        monkeypatch.setattr(client, "eval", fail_cleanup)

        @SingleExecute(lock_id="error", redis_client=client)
        async def work():
            raise ValueError("business failure")

        with pytest.raises(ValueError, match="business failure"):
            await work()
    finally:
        await client.aclose()


async def test_single_execute_detects_fast_ownership_loss():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:

        @SingleExecute(lock_id="fast", redis_client=client, cooldown_sec=0)
        async def work():
            await client.set("singleexec:fast", "new-owner", ex=60)
            return 1

        with pytest.raises(RuntimeError, match="lease lost"):
            await work()
        assert await client.get("singleexec:fast") == "new-owner"
    finally:
        await client.aclose()


def test_single_execute_sync_detects_fast_ownership_loss():
    client = fakeredis.FakeRedis(decode_responses=True)
    try:

        @SingleExecute(lock_id="fast", redis_client=client, cooldown_sec=0)
        def work():
            client.set("singleexec:fast", "new-owner", ex=60)
            return 1

        with pytest.raises(RuntimeError, match="lease lost"):
            work()
        assert client.get("singleexec:fast") == "new-owner"
    finally:
        client.close()


def test_single_execute_rejects_async_client_for_sync_function():
    client = fakeredis.aioredis.FakeRedis()
    with pytest.raises(ValueError, match="client"):
        SingleExecute(lock_id="bad", redis_client=client)(lambda: 1)


async def test_single_execute_outer_decorator_updates_registered_worker():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:

        @SingleExecute(lock_id="registered", redis_client=client, cooldown_sec=0)
        @kafka_handler(name="locked", topics_in=["locked.in"], topics_out=["locked.out"])
        async def work(*args):
            return 1

        worker = next(w for w in all_workers() if w.name == "locked")
        await client.set("singleexec:registered", "other", ex=60)
        assert await worker.fn() is None
    finally:
        await client.aclose()


async def test_cancelled_single_execute_preserves_cancellation_and_releases():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    entered = asyncio.Event()
    try:

        @SingleExecute(lock_id="cancel", redis_client=client, cooldown_on_error_sec=0)
        async def work():
            entered.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(work())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not await client.exists("singleexec:cancel")
    finally:
        await client.aclose()


async def test_inflight_success_does_not_close_a_newly_opened_circuit():
    entered, release = asyncio.Event(), asyncio.Event()

    @call_circuit_breaker(fail_max=1, reset_timeout=30)
    async def work(fail=False):
        if fail:
            raise ValueError("dependency failed")
        entered.set()
        await release.wait()
        return 1

    older = asyncio.create_task(work())
    await entered.wait()
    with pytest.raises(ValueError):
        await work(fail=True)
    release.set()
    assert await older == 1
    with pytest.raises(CircuitOpenError):
        await work()


@pytest.mark.parametrize(
    "size,expected", [(0, 1), (255, 1), (256, 2), (65535, 2), (65536, 3), (2**32, 5)]
)
def test_byte_length_boundaries(size, expected):
    assert loi(size) == expected


@pytest.mark.parametrize(
    "command,data,le",
    [
        ("00 a4 04 00", "", ""),
        ("00 a4 04 00 00", "", "00"),
        ("00 a4 04 00 02 aa bb", "aa bb", ""),
        ("00 a4 04 00 02 aa bb 7f", "aa bb", "7f"),
        ("00 a4 04 00 00 01 00", "", "00 01 00"),
        ("00 a4 04 00 00 00 02 aa bb 00 10", "aa bb", "00 10"),
    ],
)
def test_short_and_extended_apdu_cases(command, data, le):
    assert apdu_expand(command) == ("00 a4 04 00", data, le)


@pytest.mark.parametrize("length", [1, 2, 254, 255, 256, 1024, 65535])
def test_apdu_roundtrip_boundaries(length):
    data = " ".join(["ab"] * length)
    le = "00 10" if length > 255 else "10"
    command = apdu_compact("00 a4 04 00", data, le)
    assert apdu_expand(command) == ("00 a4 04 00", data, le)


def test_invalid_non_byte_text_is_rejected_instead_of_corrupted():
    with pytest.raises(UnicodeEncodeError):
        s2b("\u1000")


def test_nested_lowercase_and_falsy_values_are_preserved():
    source = {"PAYLOAD": {"CaseSensitive": "Keep"}, 7: {"NAME": "value"}}
    assert lower_rx(source) == {"payload": {"CaseSensitive": "Keep"}, 7: {"name": "value"}}
    assert nth_obj([{"count": 0}], "count") == 0
