"""2.1 worker declarations, raw mode, observers, supervision and tracing details."""

import asyncio
import json
import sys
import types
from collections import Counter
from dataclasses import replace

import pytest
from aiokafka import TopicPartition
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from conftest import Consumer, Producer, record
from framework.decorators import kafka_handler, retry_to_dlq
from framework.decorators.kafka_workers import all_workers, collect_workers, validate_workers
from framework.etl.framework_etl import KafkaRuntime, MissingTopicsError
from framework.etl.observer import WorkerObserver
from framework.etl.outcomes import Outcome
from framework.etl.processing import Processor
from framework.etl.supervisor import KafkaSupervisor
from framework.observability import Metrics
from framework.tracing import tracing


class Topology:
    """Workers declared on methods, with deferred values read from a resolver."""

    def __init__(self, prefix):
        self.prefix = prefix
        self.seen = []

    @retry_to_dlq(dlq_topic="dead_letter", max_attempts=2)
    @kafka_handler(
        name="bigconnect",
        topics_in=lambda r: [f"{r['prefix']}-bigconnect"],
        topics_out=["output-data-importer"],
        consumer_group=lambda r: f"sd-{r['env']}-bigconnect",
        bulk_mode=True,
        batch_size=lambda r: r.get("batch", 100),
        with_context=True,
    )
    async def bigconnect(self, messages, consumer_name, metadatas, context):
        self.seen.append(("bigconnect", consumer_name, len(messages)))
        return [Outcome.skip("seen") for _ in messages]

    @kafka_handler(name="stt", topics_in=["stt-stream"], topics_out=[], raw=True)
    @retry_to_dlq(dlq_topic="stt-dlq")
    async def stt(self, message, consumer_name, metadatas):
        self.seen.append(("stt", message))

    @kafka_handler(
        name="export",
        topics_in=["export"],
        topics_out=["out"],
        enabled=lambda r: r.get("export", False),
    )
    def export(self, message, consumer_name, metadatas):
        return message

    def not_a_worker(self):
        return None


RESOLVER = {"prefix": "src", "env": "test"}


def test_method_workers_are_declared_not_registered():
    names = {w.name for w in all_workers()}
    assert not {"bigconnect", "stt", "export"} & names


def test_collect_workers_binds_methods_and_resolves_deferred_values():
    topology = Topology("x")
    specs = {s.name: s for s in collect_workers(topology, RESOLVER)}
    assert set(specs) == {"bigconnect", "export", "stt"}
    big = specs["bigconnect"]
    assert big.topics_in == ["src-bigconnect"] and big.consumer_group == "sd-test-bigconnect"
    assert big.batch_size == 100 and big.bulk_mode and big.with_context
    assert big.retry_to_dlq.dlq_topic == "dead_letter" and big.retry_to_dlq.max_attempts == 2
    assert big.fn.__self__ is topology
    assert specs["stt"].retry_to_dlq.dlq_topic == "stt-dlq"  # policy below the declaration
    assert specs["stt"].raw and specs["stt"].topics_out == []
    assert specs["export"].enabled is False


def test_collect_workers_override_hook_and_validation():
    def override(spec):
        return replace(spec, max_workers=9) if spec.name == "stt" else spec

    specs = {
        s.name: s
        for s in collect_workers(Topology("x"), {**RESOLVER, "export": True}, override=override)
    }
    assert specs["stt"].max_workers == 9 and specs["export"].enabled
    with pytest.raises(ValueError):
        collect_workers(Topology("x"), {**RESOLVER, "batch": 1})  # bulk needs batch_size >= 2


def test_collect_workers_on_a_module_with_deferred_functions():
    module = types.ModuleType("deferred_workers")
    exec(
        "from framework.decorators import kafka_handler\n"
        "@kafka_handler(name='m', topics_in=lambda r: [r], topics_out=[])\n"
        "async def handle(message, consumer_name, metadatas):\n"
        "    return None\n",
        module.__dict__,
    )
    sys.modules["deferred_workers"] = module
    try:
        (spec,) = collect_workers(module, "topic-a")
        assert spec.topics_in == ["topic-a"] and spec.fn is module.handle
        assert "m" not in {w.name for w in all_workers()}
    finally:
        del sys.modules["deferred_workers"]


def test_one_topic_may_be_read_by_two_groups_but_not_twice_in_one(spec):
    a = replace(spec, name="a", topics_in=["t"])
    b = replace(spec, name="b", topics_in=["t"], consumer_group="other")
    assert len(validate_workers([a, b])) == 2
    with pytest.raises(RuntimeError, match="already handled"):
        validate_workers([a, replace(b, consumer_group=None)])


# ---------------------------------------------------------------- raw
async def test_raw_workers_receive_undecoded_bytes(settings, spec):
    seen = []

    async def handler(message, *_):
        seen.append(message)

    producer = Producer()
    worker = replace(spec, fn=handler, raw=True, topics_out=[])
    await Processor(settings, producer, Metrics()).handle(
        worker, [record(value=b"not json"), record(1, value=None)], lambda: True
    )
    assert seen == [b"not json", b""] and not producer.sent


# ------------------------------------------------------------ runtime
class TopicConsumer(Consumer):
    def __init__(self, topic, records=()):
        super().__init__(records)
        self.tp = TopicPartition(topic, 0)


async def eventually(predicate, timeout=3):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


class Recorder(WorkerObserver):
    def __init__(self):
        self.jobs, self.lags, self.states, self.restarts = [], [], [], []
        self.transient, self.recovered = [], []

    def on_job(self, report):
        self.jobs.append(report)

    def on_lag(self, group, topic, partition, lag):
        self.lags.append((group, topic, partition, lag))

    def on_state(self, worker, state, detail):
        self.states.append((worker, state, detail))

    def on_restart(self, workers, delay, error):
        self.restarts.append((tuple(workers), delay, type(error).__name__ if error else None))

    def on_transient(self, worker, wait, delay, error):
        self.transient.append((worker, wait))

    def on_recovered(self, worker, waits):
        self.recovered.append((worker, waits))


async def test_same_topic_in_two_groups_is_consumed_by_both(settings, spec, fake_module):
    seen = []

    async def handler(message, group, metadatas):
        seen.append(group)

    a = replace(spec, name="a", fn=handler)
    b = replace(spec, name="b", fn=handler, consumer_group="audit")
    main = TopicConsumer("in", [record(0)])
    other = TopicConsumer("in", [record(0)])
    runtime = KafkaRuntime(
        settings,
        consumer=main,
        producer=Producer(),
        workers=[a, b],
        consumer_factory=lambda lane: other,
    )
    await runtime.start()
    try:
        await eventually(lambda: len(seen) == 2 and main.commits and other.commits)
        assert set(runtime.partitions) == {main.tp, (1, other.tp)}
    finally:
        await runtime.stop()
    assert sorted(seen) == ["audit", "test"]


async def test_observer_gets_job_reports_with_outcome_labels(settings, spec, fake_module):
    async def handler(messages, *_):
        return [Outcome.ok(label="stored"), Outcome.skip("dup"), Outcome.ok(label="stored")]

    observer = Recorder()
    worker = replace(spec, fn=handler, bulk_mode=True, batch_size=3, batch_timeout_ms=1)
    consumer = Consumer([record(i) for i in range(3)])
    runtime = KafkaRuntime(
        settings, consumer=consumer, producer=Producer(), workers=[worker], observer=observer
    )
    await runtime.start()
    try:
        await eventually(lambda: observer.jobs and observer.lags)
    finally:
        await runtime.stop()
    (report,) = observer.jobs
    assert report.records == 3 and report.ok and report.topic == "in"
    assert report.outcomes == {"stored": 2, "skipped": 1}
    assert observer.lags[-1][:3] == ("test", "in", 0)


async def test_observer_sees_transient_waits_and_recovery(settings, spec):
    class Down(Exception):
        pass

    calls = 0

    async def handler(message, *_):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise Down()

    class Zero:
        def delay(self, attempt):
            return 0

    observer = Recorder()
    policy = retry_to_dlq(dlq_topic="d", transient=(Down,), transient_backoff=Zero())(lambda: None)
    worker = replace(spec, fn=handler, topics_out=[], retry_to_dlq=policy._qsint_retry_to_dlq)
    await Processor(settings, Producer(), Metrics(), observer=observer).handle(
        worker, [record()], lambda: True
    )
    assert observer.transient == [("test", 0), ("test", 1)] and observer.recovered == [("test", 2)]


async def test_a_broken_observer_never_breaks_processing(settings, spec, fake_module):
    class Broken(WorkerObserver):
        def on_job(self, report):
            raise RuntimeError("observer bug")

    consumer = Consumer([record(0)])
    runtime = KafkaRuntime(
        settings, consumer=consumer, producer=Producer(), workers=[spec], observer=Broken()
    )
    await runtime.start()
    try:
        await eventually(lambda: consumer.commits.get(consumer.tp) == 1)
        assert runtime.error is None
    finally:
        await runtime.stop()


async def test_enabled_false_workers_are_skipped(settings, spec, fake_module):
    off = replace(spec, name="off", topics_in=["other"], enabled=False)
    runtime = KafkaRuntime(settings, consumer=Consumer(), producer=Producer(), workers=[spec, off])
    await runtime.start()
    try:
        assert [w.name for w in runtime.workers] == ["test"] and runtime.disabled == ["off"]
    finally:
        await runtime.stop()


# ----------------------------------------------------------- tracing
async def test_kafka_operation_spans_are_opt_in(settings, spec, monkeypatch, fake_module):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(tracing, "_tracer", provider.get_tracer("test"))
    consumer = Consumer([record(0)])
    runtime = KafkaRuntime(
        settings.model_copy(update={"trace_kafka_operations": True}),
        consumer=consumer,
        producer=Producer(),
        workers=[spec],
    )
    await runtime.start()
    try:
        await eventually(lambda: consumer.commits.get(consumer.tp) == 1)
    finally:
        await runtime.stop()
    names = Counter(span.name for span in exporter.get_finished_spans())
    assert (
        names["kafka.poll"] == 1 and names["kafka.publish"] == 1 and names["kafka.process in"] == 1
    )
    poll = next(s for s in exporter.get_finished_spans() if s.name == "kafka.poll")
    assert poll.attributes["messaging.consumer.group.name"] == "test"
    provider.shutdown()


# --------------------------------------------------------- supervisor
class FakeRuntime:
    """Runtime double: fails, runs or reports missing topics as scripted."""

    plans: dict = {}
    started: list = []

    def __init__(self, settings, workers, metrics, observer):
        self.names = tuple(w.name for w in workers)
        self.error = None
        self.task = None
        self.stopped = False

    async def start(self):
        plan = FakeRuntime.plans[self.names].pop(0) if FakeRuntime.plans.get(self.names) else "run"
        FakeRuntime.started.append(self.names)
        if plan == "unreachable":
            raise ConnectionError("broker unreachable")
        if plan == "missing":
            raise MissingTopicsError({"t-missing"})
        loop = asyncio.get_running_loop()
        self.task = loop.create_future()
        if plan == "crash":
            self.error = RuntimeError("crashed")
            self.task.set_result(None)

    async def stop(self):
        self.stopped = True
        if self.task is not None and not self.task.done():
            self.task.set_result(None)


@pytest.fixture
def fake_runtimes():
    FakeRuntime.plans, FakeRuntime.started = {}, []
    return FakeRuntime


def unit_workers(spec):
    a = replace(spec, name="a", topics_in=["ta"], consumer_group="ga")
    b = replace(spec, name="b", topics_in=["tb"], consumer_group="gb")
    c = replace(spec, name="c", topics_in=["tc"], consumer_group="gc", enabled=False)
    return [a, b, c]


async def test_supervisor_restarts_a_failing_unit_alone_with_backoff(settings, spec, fake_runtimes):
    fake_runtimes.plans[("a",)] = ["crash"] * 6 + ["run"]
    sleeps = []
    gate = asyncio.Event()

    async def sleep(delay):
        sleeps.append(delay)
        await asyncio.sleep(0)

    observer = Recorder()
    supervisor = KafkaSupervisor(
        settings, unit_workers(spec), observer=observer, runtime_factory=fake_runtimes, sleep=sleep
    )
    await supervisor.start()
    try:
        await eventually(
            lambda: supervisor.status()["a"]["state"] == "running" and len(sleeps) == 6
        )
        assert sleeps == [5.0, 10.0, 20.0, 40.0, 60.0, 60.0]
        status = supervisor.status()
        assert status["a"]["restarts"] == 6 and status["b"]["restarts"] == 0
        assert status["b"]["state"] == "running" and status["c"]["state"] == "disabled"
        assert fake_runtimes.started.count(("b",)) == 1 and ("c",) not in fake_runtimes.started
        assert supervisor.ready is True
        assert len(observer.restarts) == 6 and observer.restarts[0] == (("a",), 5.0, "RuntimeError")
        gate.set()
    finally:
        await supervisor.stop()
    assert supervisor.status()["a"]["state"] == "stopped"
    assert ("c", "disabled", None) in observer.states


async def test_supervisor_reports_backing_off_and_missing_topics(settings, spec, fake_runtimes):
    fake_runtimes.plans[("a",)] = ["unreachable"] * 100
    fake_runtimes.plans[("b",)] = ["missing"] * 100
    release = asyncio.Event()

    async def sleep(delay):
        await release.wait()

    supervisor = KafkaSupervisor(
        settings, unit_workers(spec)[:2], runtime_factory=fake_runtimes, sleep=sleep
    )
    await supervisor.start()
    try:
        await eventually(
            lambda: (
                supervisor.status()["a"]["state"] == "backing_off"
                and supervisor.status()["b"]["state"] == "topic_missing"
            )
        )
        assert supervisor.status()["a"]["detail"] == "ConnectionError"
        assert supervisor.status()["b"]["detail"] == "t-missing"
        assert supervisor.ready is False and supervisor.error is None
    finally:
        release.set()
        await supervisor.stop()


def test_supervisor_groups_workers_by_consumer(settings, spec):
    a = replace(spec, name="a", topics_in=["t1"])
    b = replace(spec, name="b", topics_in=["t2"])
    c = replace(spec, name="c", topics_in=["t3"], consumer_group="g2")
    supervisor = KafkaSupervisor(settings, [a, b, c])
    assert sorted(len(u.workers) for u in supervisor.units) == [1, 2]
    with pytest.raises(ValueError, match="nope"):
        KafkaSupervisor(settings.model_copy(update={"disabled_workers": ["nope"]}), [a])


async def test_supervisor_runs_real_runtimes_per_unit(settings, spec, fake_module):
    seen = []

    async def handler(message, group, metadatas):
        seen.append(group)

    a = replace(spec, name="a", fn=handler)
    b = replace(spec, name="b", topics_in=["side"], consumer_group="g2", fn=handler)
    consumers = {
        "test": Consumer([record(0)]),
        "g2": TopicConsumer("side", [record(0, topic="side")]),
    }

    def factory(settings_, workers, metrics, observer):
        group = workers[0].consumer_group or settings_.consumer_name
        return KafkaRuntime(
            settings_,
            consumer=consumers[group],
            producer=Producer(),
            workers=workers,
            metrics=metrics,
            observer=observer,
        )

    supervisor = KafkaSupervisor(settings, [a, b], runtime_factory=factory)
    await supervisor.start()
    try:
        await eventually(lambda: sorted(seen) == ["g2", "test"])
        await eventually(lambda: all(c.commits for c in consumers.values()))
    finally:
        await supervisor.stop()
    assert all(c.stopped for c in consumers.values())


def test_json_round_trip_of_status(settings, spec):
    supervisor = KafkaSupervisor(settings, unit_workers(spec))
    assert json.loads(json.dumps(supervisor.status()))["c"]["state"] == "disabled"
