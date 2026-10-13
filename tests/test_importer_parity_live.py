"""2.1 features against a real broker. QF_INTEGRATION=1 python -m pytest tests/test_importer_parity_live.py."""

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
from framework.etl.dead_letter import HeaderDeadLetter
from framework.etl.framework_etl import KafkaRuntime, MissingTopicsError
from framework.etl.outcomes import Outcome
from framework.streams.kafka_client import KafkaClient

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("QF_INTEGRATION") != "1", reason="Set QF_INTEGRATION=1 for live services"
    ),
]
BOOTSTRAP = os.getenv("QF_TEST_KAFKA", "localhost:29092")


@pytest.fixture
async def topics():
    suffix = uuid.uuid4().hex[:12]
    names = {name: f"qf-parity-{suffix}-{name}" for name in ("in", "side", "out", "o2", "dlq")}
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(t, 2, 1) for t in names.values()])
        yield names
        await admin.delete_topics(list(names.values()))
    finally:
        await admin.close()


@pytest.fixture
async def producer():
    client = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    await client.start()
    try:
        yield client
    finally:
        await client.stop()


def settings_for(topics, **kwargs):
    return FrameworkSettings(
        worker_modules=["json"],
        kafka_bootstrap_servers=BOOTSTRAP,
        consumer_name=topics["in"],
        error_topic=topics["dlq"],
        retry_base_sec=0.005,
        retry_max_sec=0.05,
        commit_tick_sec=0.02,
        **kwargs,
    )


def dlq_policy(topics, **kwargs):
    return RetryToDlqConfig(
        max_attempts=1,
        dlq_topic=topics["dlq"],
        dlq_format=HeaderDeadLetter(worker_header="flow"),
        **kwargs,
    )


async def read(topic, count, timeout=30):
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=BOOTSTRAP, auto_offset_reset="earliest", enable_auto_commit=False
    )
    found = []
    await consumer.start()
    try:
        async with asyncio.timeout(timeout):
            while len(found) < count:
                batch = await consumer.getmany(timeout_ms=100)
                found.extend(r for records in batch.values() for r in records)
        found.extend(
            r for records in (await consumer.getmany(timeout_ms=300)).values() for r in records
        )
        return found
    finally:
        await consumer.stop()


async def committed(group, topic, partitions=2):
    consumer = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP, group_id=group, enable_auto_commit=False
    )
    await consumer.start()
    try:
        return {p: await consumer.committed(TopicPartition(topic, p)) for p in range(partitions)}
    finally:
        await consumer.stop()


async def until(predicate, timeout=30):
    async with asyncio.timeout(timeout):
        while not await predicate():
            await asyncio.sleep(0.1)


async def test_split_arrays_header_dead_letters_and_commits(spec, topics, producer):
    async def handler(message, group, metadatas, context):
        return Outcome.ok({**message, "element": context.element_index})

    worker = replace(
        spec,
        topics_in=[topics["in"]],
        topics_out=[topics["out"]],
        fn=handler,
        split_arrays=True,
        with_context=True,
        inject_id=False,
        key_by=None,
        retry_to_dlq=dlq_policy(topics),
    )
    await producer.send_and_wait(topics["in"], b'[{"a": 1}, {"b": 2}]', key=b"k", partition=0)
    await producer.send_and_wait(topics["in"], b"{bad", key=b"bad", partition=0)
    runtime = KafkaRuntime(settings_for(topics), workers=[worker])
    await runtime.start()
    try:
        outputs = await read(topics["out"], 2)
        dead = await read(topics["dlq"], 1)
        await until(lambda: _committed_at_least(topics["in"], topics["in"], {0: 2}))
    finally:
        await runtime.stop()
    assert sorted((json.loads(r.value)["element"], r.key) for r in outputs) == [
        (0, b"k"),
        (1, b"k"),
    ]
    (letter,) = dead
    headers = {k: v.decode() for k, v in letter.headers}
    assert letter.value == b"{bad" and letter.key == b"bad"
    assert headers["stage"] == "decode" and headers["flow"] == spec.name
    assert headers["source_topic"] == topics["in"] and headers["source_offset"] == "1"


async def _committed_at_least(group, topic, expected):
    actual = await committed(group, topic)
    return all(actual[p] is not None and actual[p] >= n for p, n in expected.items())


async def test_each_consumer_group_commits_on_its_own(spec, topics, producer):
    seen = []

    async def handler(message, group, metadatas):
        seen.append(group)

    group_b = f"{topics['side']}-group"
    a = replace(spec, name="a", topics_in=[topics["in"]], topics_out=[], fn=handler)
    b = replace(
        spec,
        name="b",
        topics_in=[topics["side"]],
        topics_out=[],
        fn=handler,
        consumer_group=group_b,
    )
    await producer.send_and_wait(topics["in"], b'{"n": 1}', partition=0)
    await producer.send_and_wait(topics["side"], b'{"n": 2}', partition=1)
    runtime = KafkaRuntime(settings_for(topics), workers=[a, b])
    await runtime.start()
    try:
        await until(lambda: _committed_at_least(topics["in"], topics["in"], {0: 1}))
        await until(lambda: _committed_at_least(group_b, topics["side"], {1: 1}))
    finally:
        await runtime.stop()
    assert sorted(seen) == sorted([topics["in"], group_b])
    # The default group never touched the other worker's topic.
    assert (await committed(topics["in"], topics["side"])) == {0: None, 1: None}


async def test_latest_offset_reset_skips_history(spec, topics, producer):
    seen = []

    async def handler(message, *_):
        seen.append(message["n"])

    worker = replace(
        spec,
        topics_in=[topics["in"]],
        topics_out=[],
        fn=handler,
        auto_offset_reset="latest",
        consumer_group=f"{topics['in']}-latest",
    )
    await producer.send_and_wait(topics["in"], b'{"n": "old"}', partition=0)
    runtime = KafkaRuntime(settings_for(topics), workers=[worker])
    await runtime.start()
    try:
        await asyncio.sleep(3)  # join the group and fetch the end offsets
        await producer.send_and_wait(topics["in"], b'{"n": "new"}', partition=0)

        async def got_new():
            return "new" in seen

        await until(got_new)
    finally:
        await runtime.stop()
    assert seen == ["new"]


async def test_context_carries_the_producer_timestamp_and_headers(spec, topics):
    seen = []

    async def handler(message, group, metadatas, context):
        seen.append(context)

    worker = replace(spec, topics_in=[topics["in"]], topics_out=[], fn=handler, with_context=True)
    async with KafkaClient(BOOTSTRAP) as client:
        await client.put_message(
            topics["in"],
            {"id": "x"},
            key="k1",
            headers={"replayed_from": "dead_letter"},
            timestamp_ms=1700000000000,
        )
    runtime = KafkaRuntime(settings_for(topics), workers=[worker])
    await runtime.start()
    try:

        async def got():
            return bool(seen)

        await until(got)
    finally:
        await runtime.stop()
    (context,) = seen
    assert context.timestamp_ms == 1700000000000 and context.timestamp_type == 0
    assert context.key == b"k1" and context.header("replayed_from") == b"dead_letter"


async def test_oversized_output_dead_letters_its_message_and_commits(spec, topics, producer):
    async def handler(message, *_):
        return Outcome.ok({"id": "big", "blob": "x" * 4000})

    worker = replace(
        spec,
        topics_in=[topics["in"]],
        topics_out=[topics["out"]],
        fn=handler,
        retry_to_dlq=dlq_policy(topics),
    )
    await producer.send_and_wait(topics["in"], b'{"n": 1}', partition=0)
    runtime = KafkaRuntime(settings_for(topics, producer_max_request_size=2048), workers=[worker])
    await runtime.start()
    try:
        (letter,) = await read(topics["dlq"], 1)
        await until(lambda: _committed_at_least(topics["in"], topics["in"], {0: 1}))
    finally:
        await runtime.stop()
    headers = {k: v.decode() for k, v in letter.headers}
    # The document is too large for the dead-letter topic too: a reference replaces it.
    assert json.loads(letter.value)["_truncated"] is True
    assert headers["stage"] == "forward" and headers["target_topic"] == topics["out"]


class Unavailable(Exception):
    pass


async def test_transient_failures_hold_the_commit_until_success(spec, topics, producer):
    attempts = 0
    release = asyncio.Event()

    async def handler(message, *_):
        nonlocal attempts
        attempts += 1
        if not release.is_set():
            raise Unavailable("store down")

    class Fast:
        def delay(self, attempt):
            return 0.05

    worker = replace(
        spec,
        topics_in=[topics["in"]],
        topics_out=[],
        fn=handler,
        retry_to_dlq=dlq_policy(topics, transient=(Unavailable,), transient_backoff=Fast()),
    )
    await producer.send_and_wait(topics["in"], b'{"n": 1}', partition=0)
    runtime = KafkaRuntime(settings_for(topics), workers=[worker])
    await runtime.start()
    try:

        async def retrying():
            return attempts >= 5

        await until(retrying)
        assert (await committed(topics["in"], topics["in"]))[0] is None
        release.set()
        await until(lambda: _committed_at_least(topics["in"], topics["in"], {0: 1}))
    finally:
        await runtime.stop()
    consumer = AIOKafkaConsumer(
        topics["dlq"], bootstrap_servers=BOOTSTRAP, auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        assert not any((await consumer.getmany(timeout_ms=500)).values())
    finally:
        await consumer.stop()


async def test_verify_topics_refuses_missing_topics_without_creating_them(spec, topics):
    missing = f"{topics['in']}-missing"
    worker = replace(spec, topics_in=[topics["in"]], topics_out=[missing])
    runtime = KafkaRuntime(settings_for(topics, verify_topics=True), workers=[worker])
    with pytest.raises(MissingTopicsError) as error:
        await runtime.start()
    assert error.value.missing == [missing]
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    await admin.start()
    try:
        assert missing not in await admin.list_topics()
    finally:
        await admin.close()
