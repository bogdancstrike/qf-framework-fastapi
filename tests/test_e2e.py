"""Real Kafka/Redis tests. QF_INTEGRATION=1 python -m pytest tests/test_e2e.py -v."""

import asyncio
import json
import os
import uuid
from dataclasses import replace

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from framework.config import FrameworkSettings
from framework.decorators.intern import RetryToDlqConfig
from framework.etl.framework_etl import KafkaRuntime
from framework.redis.aggregation import AggregationStore

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("QF_INTEGRATION") != "1", reason="Set QF_INTEGRATION=1 for live services"
    ),
]
BOOTSTRAP = os.getenv("QF_TEST_KAFKA", "localhost:29092")
REDIS = os.getenv("QF_TEST_REDIS", "redis://localhost:26379/0")


@pytest.fixture
async def broker():
    suffix = uuid.uuid4().hex[:12]
    topics = [f"qf-test-{suffix}-{name}" for name in ("in", "b", "out", "dlq")]
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, enable_idempotence=True)
    try:
        await admin.start()
        await admin.create_topics([NewTopic(t, 2, 1) for t in topics])
        await producer.start()
        yield topics, producer
    finally:
        await producer.stop()
        try:
            await admin.delete_topics(topics)
        finally:
            await admin.close()


async def read_outputs(topic, count, timeout=30):
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=BOOTSTRAP, auto_offset_reset="earliest", enable_auto_commit=False
    )
    result = []
    try:
        await consumer.start()
        async with asyncio.timeout(timeout):
            while len(result) < count:
                records = await consumer.getmany(timeout_ms=100)
                result.extend(json.loads(r.value) for batch in records.values() for r in batch)
        # Detect unintended extra output after the expected count.
        records = await consumer.getmany(timeout_ms=200)
        result.extend(json.loads(r.value) for batch in records.values() for r in batch)
        return result
    finally:
        await consumer.stop()


def runtime_settings(topics, **kwargs):
    return FrameworkSettings(
        worker_modules=["json"],
        kafka_bootstrap_servers=BOOTSTRAP,
        consumer_name=topics[0],
        error_topic=topics[3],
        redis_url=REDIS,
        retry_base_sec=0.005,
        retry_max_sec=0.05,
        pending_max_per_tp=32,
        commit_tick_sec=0.02,
        **kwargs,
    )


async def wait_committed(runtime, expected):
    async with asyncio.timeout(20):
        while True:
            actual = {tp: await runtime.consumer.committed(tp) for tp in expected}
            if all(
                actual[tp] is not None and actual[tp] >= offset for tp, offset in expected.items()
            ):
                return
            await asyncio.sleep(0.02)


@pytest.mark.parametrize("mode", ["async", "sync", "bulk"])
async def test_real_single_and_bulk_pipeline(broker, spec, mode):
    topics, producer = broker

    async def async_worker(payload, *_):
        await asyncio.sleep(0.001)
        return payload

    def sync_worker(payload, *_):
        return payload

    worker = replace(
        spec,
        topics_in=[topics[0]],
        topics_out=[topics[2]],
        fn=sync_worker if mode == "sync" else async_worker,
        bulk_mode=mode == "bulk",
        batch_size=16,
        batch_timeout_ms=10,
    )
    runtime = KafkaRuntime(runtime_settings(topics), workers=[worker])
    await runtime.start()
    try:
        futures = [
            await producer.send(topics[0], json.dumps({"id": str(i)}).encode(), partition=i % 2)
            for i in range(101)
        ]
        sent = await asyncio.gather(*futures)
        outputs = await read_outputs(topics[2], 101)
        assert len(outputs) == 101
        assert {v["id"] for v in outputs} == {str(i) for i in range(101)}
        expected = {}
        for metadata in sent:
            expected[TopicPartition(metadata.topic, metadata.partition)] = metadata.offset + 1
        await wait_committed(runtime, expected)
        assert runtime.error is None
    finally:
        await runtime.stop()


async def test_real_retry_dlq_and_poison_record(broker, spec):
    topics, producer = broker
    calls = {}

    async def handler(message, *_):
        calls[message["id"]] = calls.get(message["id"], 0) + 1
        if message.get("permanent") or calls[message["id"]] == 1:
            raise ValueError("injected")
        return message

    worker = replace(
        spec,
        fn=handler,
        topics_in=[topics[0]],
        topics_out=[topics[2]],
        retry_to_dlq=RetryToDlqConfig(max_attempts=3, dlq_topic=topics[3]),
    )
    runtime = KafkaRuntime(runtime_settings(topics), workers=[worker])
    await runtime.start()
    try:
        for value in [b'{"id":"good"}', b'{"id":"bad","permanent":true}', b"{bad"]:
            await producer.send_and_wait(topics[0], value, partition=0)
        assert (await read_outputs(topics[2], 1))[0]["id"] == "good"
        dlq = await read_outputs(topics[3], 2)
        assert sorted(v["attempts"] for v in dlq) == [0, 3]
        assert calls == {"good": 2, "bad": 3}
        await wait_committed(runtime, {TopicPartition(topics[0], 0): 3})
    finally:
        await runtime.stop()


async def test_real_aggregation_duplicate_arrivals(broker, spec):
    topics, producer = broker
    worker = replace(
        spec, kind="aggregator", topics_in=topics[:2], topics_out=[topics[2]], aggregate_by="id"
    )
    runtime = KafkaRuntime(runtime_settings(topics), workers=[worker])
    await runtime.start()
    try:
        futures = []
        for i in range(20):
            for _ in range(2):
                futures.append(
                    await producer.send(topics[0], json.dumps({"id": str(i), "a": i}).encode())
                )
                futures.append(
                    await producer.send(topics[1], json.dumps({"id": str(i), "b": i}).encode())
                )
        await asyncio.gather(*futures)
        outputs = await read_outputs(topics[2], 20)
        assert len(outputs) == 20
        assert all(v["a"] == v["b"] for v in outputs)
    finally:
        await runtime.stop()


async def test_real_redis_lease_renewal(settings, spec):
    settings.redis_url = __import__("pydantic").SecretStr(REDIS)
    settings.aggregate_lease_sec = 3
    store = AggregationStore(settings)
    spec = replace(spec, name="lease-" + uuid.uuid4().hex, topics_in=["a", "b"])
    await store.start()
    try:
        await store.claim(spec, "1", "a", {"a": 1})
        claim = await store.claim(spec, "1", "b", {"b": 1})
        async with store.heartbeat(claim):
            await asyncio.sleep(3.2)
            assert await store.client.get(claim.keys[1]) == claim.token
            assert not claim.lost
        await store.finish(claim, 1)
    finally:
        await store.client.delete(*store.keys(spec, "1"))
        await store.stop()


async def test_real_stress_capacity_transient_failures(broker, spec):
    topics, producer = broker
    count = int(os.getenv("QF_STRESS_MESSAGES", "1000"))
    attempts = {}

    async def handler(message, *_):
        n = message["n"]
        attempts[n] = attempts.get(n, 0) + 1
        await asyncio.sleep(0.001)
        if n % 10 == 0 and attempts[n] == 1:
            raise ValueError("transient")
        return message

    worker = replace(spec, fn=handler, max_workers=8, topics_in=[topics[0]], topics_out=[topics[2]])
    settings = runtime_settings(topics)
    settings.pending_max_per_tp = 16
    runtime = KafkaRuntime(settings, workers=[worker])
    await runtime.start()
    samples = []

    async def sample():
        while True:
            samples.extend(len(p.progress.offsets) for p in runtime.partitions.values())
            assert len(runtime.active[worker.name]) <= 8
            await asyncio.sleep(0.001)

    sampler = asyncio.create_task(sample())
    try:
        futures = [
            await producer.send(
                topics[0], json.dumps({"id": str(i), "n": i}).encode(), partition=i % 2
            )
            for i in range(count)
        ]
        await asyncio.gather(*futures)
        outputs = await read_outputs(topics[2], count, timeout=120)
        assert len(outputs) == count and len({v["id"] for v in outputs}) == count
        assert max(samples) <= 16
        assert sum(v - 1 for v in attempts.values()) == (count + 9) // 10
        assert not runtime.error
    finally:
        sampler.cancel()
        await asyncio.gather(sampler, return_exceptions=True)
        await runtime.stop()


async def test_real_shutdown_replays_unfinished_input(broker, spec):
    topics, producer = broker
    started = asyncio.Event()

    async def slow(payload, *_):
        started.set()
        await asyncio.sleep(10)
        return payload

    settings = runtime_settings(topics, shutdown_timeout_sec=0.1)
    worker = replace(spec, fn=slow, topics_in=[topics[0]], topics_out=[topics[2]])
    first = KafkaRuntime(settings, workers=[worker])
    await first.start()
    await producer.send_and_wait(topics[0], b'{"id":"replay"}', partition=0)
    await asyncio.wait_for(started.wait(), 20)
    await first.stop()

    async def identity(payload, *_):
        return payload

    second = KafkaRuntime(settings, workers=[replace(worker, fn=identity)])
    await second.start()
    try:
        outputs = await read_outputs(topics[2], 1)
        assert [value["id"] for value in outputs] == ["replay"]
        await wait_committed(second, {TopicPartition(topics[0], 0): 1})
    finally:
        await second.stop()
