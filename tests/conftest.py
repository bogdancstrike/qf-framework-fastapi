import asyncio
from types import SimpleNamespace

import pytest
from aiokafka import TopicPartition

from framework.config import FrameworkSettings
from framework.decorators.kafka_workers import WorkerSpec


@pytest.fixture
def settings():
    return FrameworkSettings(
        worker_modules=["test_workers"],
        kafka_bootstrap_servers="localhost:29092",
        consumer_name="test",
        retry_base_sec=0,
        poll_timeout_ms=2,
        commit_tick_sec=0.01,
        shutdown_timeout_sec=0.3,
        transport_attempts=2,
        rebalance_timeout_sec=0.1,
    )


@pytest.fixture
def spec():
    async def identity(message, consumer_name, metadatas):
        return message

    return WorkerSpec(
        kind="handler",
        name="test",
        topics_in=["in"],
        topics_out=["out"],
        metadatas={},
        max_workers=4,
        fn=identity,
    )


def record(offset=0, value=b'{"id":"x"}', topic="in", partition=0, key=b"k", headers=None):
    return SimpleNamespace(
        offset=offset, value=value, topic=topic, partition=partition, key=key, headers=headers or []
    )


class Producer:
    def __init__(self, *, failures=0, ack_delay=0, fail_topic=None):
        self.failures, self.ack_delay, self.fail_topic = failures, ack_delay, fail_topic
        self.sent, self.stopped, self.calls = [], False, 0

    async def start(self):
        pass

    async def send(self, topic, value, **kwargs):
        self.calls += 1
        future = asyncio.get_running_loop().create_future()

        async def ack():
            await asyncio.sleep(self.ack_delay)
            if self.failures or topic == self.fail_topic:
                self.failures = max(0, self.failures - 1)
                future.set_exception(RuntimeError("injected ack failure"))
            else:
                self.sent.append((topic, value, kwargs))
                if not future.cancelled():
                    future.set_result(None)

        asyncio.create_task(ack())
        return future

    async def stop(self):
        self.stopped = True


class Consumer:
    def __init__(self, records=(), commit_failures=0):
        self.records = list(records)
        self.position = 0
        self.paused = set()
        self.commits, self.commit_calls = {}, []
        self.commit_failures, self.stopped = commit_failures, False
        self.listener = None
        self.tp = TopicPartition("in", 0)

    def subscribe(self, topics, listener):
        self.listener = listener

    async def start(self):
        await self.listener.on_partitions_assigned({self.tp})

    async def getmany(self, timeout_ms, max_records):
        await asyncio.sleep(timeout_ms / 1000)
        if self.tp in self.paused:
            return {}
        batch = self.records[self.position : self.position + max_records]
        self.position += len(batch)
        return {self.tp: batch} if batch else {}

    def seek(self, tp, offset):
        self.position = next(i for i, r in enumerate(self.records) if r.offset == offset)

    def pause(self, tp):
        self.paused.add(tp)

    def resume(self, tp):
        self.paused.discard(tp)

    def assignment(self):
        return {self.tp}

    def highwater(self, tp):
        return max((r.offset + 1 for r in self.records), default=0)

    async def commit(self, updates):
        self.commit_calls.append(dict(updates))
        if self.commit_failures:
            self.commit_failures -= 1
            raise RuntimeError("injected commit failure")
        self.commits.update(updates)

    async def stop(self):
        self.stopped = True


async def eventually(predicate, timeout=3):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


@pytest.fixture(autouse=True)
def clean_registry():
    from framework.decorators import kafka_workers as kw

    old_workers, old_topics = dict(kw._WORKERS_BY_NAME), dict(kw._TOPIC_TO_WORKER)
    yield
    kw._WORKERS_BY_NAME.clear()
    kw._WORKERS_BY_NAME.update(old_workers)
    kw._TOPIC_TO_WORKER.clear()
    kw._TOPIC_TO_WORKER.update(old_topics)


@pytest.fixture
def fake_module(monkeypatch):
    import importlib

    original = importlib.import_module
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name, *a, **k: (
            SimpleNamespace() if name == "test_workers" else original(name, *a, **k)
        ),
    )
