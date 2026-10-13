"""2.1 worker features: array splitting, record context, outcomes, DLQ formats, lanes."""

import asyncio
import json
import re
from dataclasses import replace
from types import SimpleNamespace

import pytest
from aiokafka import TopicPartition
from aiokafka.errors import MessageSizeTooLargeError

from conftest import Consumer, Producer
from framework.decorators.intern import RetryToDlqConfig, retry_to_dlq
from framework.decorators.kafka_workers import WorkerSpec, validate_worker
from framework.etl.backoff import ExponentialBackoff, FullJitter
from framework.etl.dead_letter import RAW, DeadLetter, DiagnosticEnvelope, HeaderDeadLetter
from framework.etl.framework_etl import KafkaRuntime
from framework.etl.outcomes import Outcome, is_outcome_result
from framework.etl.processing import Processor
from framework.etl.records import CREATE_TIME, LOG_APPEND_TIME, context_for, record_timestamp
from framework.observability import Metrics
from framework.streams.encoding import compact_json, python_json


class NoWait:
    """Backoff without delays (tests)."""

    def __init__(self):
        self.calls = []

    def delay(self, attempt):
        self.calls.append(attempt)
        return 0


def rec(
    value, *, offset=0, topic="in", partition=0, key=b"k", headers=None, ts=1790000000000, ts_type=0
):
    if not isinstance(value, (bytes, type(None))):
        value = json.dumps(value).encode()
    return SimpleNamespace(
        topic=topic,
        partition=partition,
        offset=offset,
        key=key,
        value=value,
        headers=headers or [],
        timestamp=ts,
        timestamp_type=ts_type,
    )


def header_dlq(**kwargs):
    return RetryToDlqConfig(
        max_attempts=kwargs.pop("max_attempts", 1),
        dlq_topic="dlq",
        dlq_format=HeaderDeadLetter(worker_header="flow", **kwargs.pop("fmt", {})),
        **kwargs,
    )


def headers_of(sent):
    return {k: v.decode() for k, v in sent[2]["headers"] if k != "traceparent"}


def sent_to(producer, topic):
    return [s for s in producer.sent if s[0] == topic]


class SizeLimitedProducer(Producer):
    """Refuses (permanently) values above ``limit`` bytes or sends to ``refuse_topic``."""

    def __init__(self, limit=10**9, refuse_topic=None):
        super().__init__()
        self.limit, self.refuse_topic = limit, refuse_topic

    async def send(self, topic, value, **kwargs):
        if len(value) > self.limit or topic == self.refuse_topic:
            self.calls += 1
            raise MessageSizeTooLargeError("too large")
        return await super().send(topic, value, **kwargs)


# --------------------------------------------------------------- decoding
async def test_split_arrays_gives_one_call_per_object_element(settings, spec):
    seen = []

    async def handler(message, group, metadatas, context):
        seen.append((message, context.element_index))

    producer, metrics = Producer(), Metrics()
    worker = replace(spec, fn=handler, split_arrays=True, with_context=True, inject_id=False)
    await Processor(settings, producer, metrics).handle(
        worker, [rec([{"a": 1}, "x", 5, {"b": 2}], offset=7)], lambda: True
    )
    assert seen == [({"a": 1}, 0), ({"b": 2}, 3)]
    sample = metrics.registry.get_sample_value(
        "qf_decode_dropped_total", {"worker": "test", "reason": "non_object_element"}
    )
    assert sample == 2
    assert not producer.sent


async def test_split_arrays_empty_array_is_a_skip(settings, spec):
    calls = []
    producer, metrics = Producer(), Metrics()
    worker = replace(spec, fn=lambda *a: calls.append(a), split_arrays=True)
    await Processor(settings, producer, metrics).handle(worker, [rec([])], lambda: True)
    assert not calls and not producer.sent
    assert (
        metrics.registry.get_sample_value(
            "qf_messages_total", {"worker": "test", "outcome": "skipped"}
        )
        == 1
    )


@pytest.mark.parametrize("value", [[1, "x"], 5, "x", None, True])
async def test_split_arrays_non_objects_are_dead_lettered_raw(settings, spec, value):
    producer = Producer()
    worker = replace(spec, split_arrays=True, retry_to_dlq=header_dlq())
    record = rec(json.dumps(value).encode())
    await Processor(settings, producer, Metrics()).handle(worker, [record], lambda: True)
    (dead,) = sent_to(producer, "dlq")
    assert dead[1] == record.value
    headers = headers_of(dead)
    assert headers["stage"] == "decode" and headers["reason"] == "not_an_object"


@pytest.mark.parametrize("raw", [b"{bad", b"\xff\xfe"])
async def test_split_arrays_invalid_json_is_dead_lettered_byte_identical(settings, spec, raw):
    producer = Producer()
    worker = replace(spec, split_arrays=True, retry_to_dlq=header_dlq())
    await Processor(settings, producer, Metrics()).handle(worker, [rec(raw)], lambda: True)
    (dead,) = sent_to(producer, "dlq")
    assert dead[1] == raw
    headers = headers_of(dead)
    assert headers["stage"] == "decode" and headers["reason"].startswith("invalid_json:")


async def test_without_split_arrays_an_array_keeps_the_2_0_envelope(settings, spec):
    producer = Producer()
    await Processor(settings, producer, Metrics()).handle(spec, [rec([{"a": 1}])], lambda: True)
    envelope = json.loads(producer.sent[0][1])
    assert producer.sent[0][0] == settings.error_topic
    assert set(envelope) == {"id", "source", "worker", "attempts", "error", "raw_base64"}


async def test_nan_tokens_decode_and_ids_are_not_injected_on_request(settings, spec):
    seen = []

    async def handler(message, *_):
        seen.append(message)

    worker = replace(spec, fn=handler, inject_id=False)
    await Processor(settings, Producer(), Metrics()).handle(
        worker, [rec(b'{"type": "stt", "a": 5, "b": null, "x": NaN}')], lambda: True
    )
    assert seen[0]["a"] == 5 and seen[0]["b"] is None and "id" not in seen[0]
    assert seen[0]["x"] != seen[0]["x"]  # NaN


async def test_split_array_elements_get_distinct_injected_ids(settings, spec):
    seen = []

    async def handler(message, *_):
        seen.append(message["id"])

    worker = replace(spec, fn=handler, split_arrays=True)
    await Processor(settings, Producer(), Metrics()).handle(
        worker, [rec([{"a": 1}, {"b": 2}], offset=4)], lambda: True
    )
    assert seen == ["in:0:4", "in:0:4:1"]


# ---------------------------------------------------------------- context
def test_record_timestamps_and_types():
    assert record_timestamp(rec({}, ts=1790000000000, ts_type=0)) == (1790000000000, CREATE_TIME)
    assert record_timestamp(rec({}, ts=1790000000500, ts_type=1)) == (
        1790000000500,
        LOG_APPEND_TIME,
    )
    assert record_timestamp(rec({}, ts=None)) == (-1, -1)
    assert record_timestamp(SimpleNamespace()) == (-1, -1)


def test_context_carries_coordinates_key_and_headers():
    context = context_for(
        rec({}, offset=42, partition=1, key=b"k1", headers=[("h", b"v"), ("h", b"w")]), "w", "g", 3
    )
    assert (context.topic, context.partition, context.offset, context.key) == ("in", 1, 42, b"k1")
    assert context.timestamp_ms == 1790000000000 and context.element_index == 3
    assert context.header("h") == b"w" and context.header("missing") is None
    assert context.coordinates == "in:1:42:3" and context.consumer_group == "g"


async def test_bulk_handler_receives_aligned_contexts(settings, spec):
    seen = []

    async def handler(messages, group, metadatas, context):
        seen.append(([m["n"] for m in messages], [(c.offset, c.element_index) for c in context]))
        return [Outcome.skip("seen") for _ in messages]

    worker = replace(
        spec, fn=handler, bulk_mode=True, batch_size=10, split_arrays=True, with_context=True
    )
    records = [rec([{"n": 1}, {"n": 2}], offset=0), rec({"n": 3}, offset=1)]
    await Processor(settings, Producer(), Metrics()).handle(worker, records, lambda: True)
    assert seen == [([1, 2, 3], [(0, 0), (0, 1), (1, 0)])]


async def test_sync_handler_receives_context(settings, spec):
    seen = []

    def handler(message, group, metadatas, context):
        seen.append(context.offset)

    worker = replace(spec, fn=handler, with_context=True)
    await Processor(settings, Producer(), Metrics()).handle(
        worker, [rec({}, offset=9)], lambda: True
    )
    assert seen == [9]


# ------------------------------------------------------------- outputs
async def test_key_by_dotted_path_and_source_key(settings, spec):
    async def handler(message, *_):
        return [{"system": {"_id": "d1"}}, {"other": 1}]

    producer = Producer()
    worker = replace(spec, fn=handler, key_by="system._id")
    await Processor(settings, producer, Metrics()).handle(
        worker, [rec({}, key=b"src")], lambda: True
    )
    assert [s[2]["key"] for s in producer.sent] == [b"d1", b"src"]

    producer = Producer()
    await Processor(settings, producer, Metrics()).handle(
        replace(worker, key_by=None), [rec({"id": "x"}, key=b"src")], lambda: True
    )
    assert producer.sent[0][2]["key"] == b"src"


async def test_python_json_encoder_matches_json_dumps_defaults(settings, spec):
    async def handler(message, *_):
        return {"a": "ă", "b": 1}

    producer = Producer()
    await Processor(settings, producer, Metrics()).handle(
        replace(spec, fn=handler, encoder=python_json), [rec({})], lambda: True
    )
    assert producer.sent[0][1] == b'{"a": "\\u0103", "b": 1}'
    assert compact_json({"a": 1}) == b'{"a":1}'


async def test_sink_worker_publishes_nothing_and_rejects_outputs(settings, spec):
    sink = replace(spec, topics_out=[], retry_to_dlq=header_dlq())
    producer = Producer()
    assert await Processor(settings, producer, Metrics()).handle(
        replace(sink, fn=lambda *a: None), [rec({})], lambda: True
    )
    assert not producer.sent
    producer = Producer()
    assert (
        await Processor(settings, producer, Metrics()).handle(sink, [rec({})], lambda: True)
        is False
    )
    assert "no output topics" in headers_of(sent_to(producer, "dlq")[0])["reason"]


# ------------------------------------------------------------- outcomes
def test_outcome_constructors_and_detection():
    assert Outcome.ok({"a": 1}).outputs == ({"a": 1},)
    assert not Outcome.retry().terminal and Outcome.skip("x").terminal
    with pytest.raises(TypeError):
        Outcome.ok([1])
    assert is_outcome_result(Outcome.skip("x"))
    assert is_outcome_result([Outcome.skip("x")])
    assert (
        not is_outcome_result([{"a": 1}])
        and not is_outcome_result([])
        and not is_outcome_result(None)
    )


async def test_bulk_outcomes_settle_each_message(settings, spec):
    async def handler(messages, *_):
        return [
            Outcome.ok({"id": "a", "v": 1}, label="stored"),
            Outcome.skip("duplicate"),
            Outcome.dead_letter(
                "bad shape", stage="persist", headers={"message_type": "bigconnect"}
            ),
            Outcome.dead_letter("two units", values=[{"u": 1}, {"u": 2}]),
        ]

    producer, metrics = Producer(), Metrics()
    worker = replace(spec, fn=handler, bulk_mode=True, batch_size=4, retry_to_dlq=header_dlq())
    records = [rec({"n": i}, offset=i) for i in range(4)]
    assert await Processor(settings, producer, metrics).handle(worker, records, lambda: True)
    assert [json.loads(s[1]) for s in sent_to(producer, "out")] == [{"id": "a", "v": 1}]
    dead = sent_to(producer, "dlq")
    assert dead[0][1] == records[2].value  # source bytes, unmodified
    assert headers_of(dead[0])["stage"] == "persist"
    assert headers_of(dead[0])["message_type"] == "bigconnect"
    assert [json.loads(d[1]) for d in dead[1:]] == [{"u": 1}, {"u": 2}]
    labels = {
        outcome: metrics.registry.get_sample_value(
            "qf_messages_total", {"worker": "test", "outcome": outcome}
        )
        for outcome in ("stored", "skipped", "dlq")
    }
    assert labels == {"stored": 1, "skipped": 1, "dlq": 3}


async def test_retry_outcome_reinvokes_only_that_message(settings, spec):
    calls = []

    async def handler(messages, *_):
        calls.append([m["n"] for m in messages])
        if len(calls) == 1:
            return [Outcome.ok(), Outcome.retry("busy"), Outcome.ok()]
        return [Outcome.ok({"id": "late"})]

    backoff = NoWait()
    policy = replace(header_dlq(max_attempts=3), backoff=backoff)
    worker = replace(spec, fn=handler, bulk_mode=True, batch_size=3, retry_to_dlq=policy)
    producer = Producer()
    records = [rec({"n": i}, offset=i) for i in range(3)]
    assert await Processor(settings, producer, Metrics()).handle(worker, records, lambda: True)
    assert calls == [[0, 1, 2], [1]] and backoff.calls == [0]
    assert len(sent_to(producer, "out")) == 1 and not sent_to(producer, "dlq")


async def test_retry_outcome_dead_letters_when_attempts_are_exhausted(settings, spec):
    async def handler(message, *_):
        return Outcome.retry("still locked")

    producer = Producer()
    worker = replace(
        spec, fn=handler, retry_to_dlq=replace(header_dlq(max_attempts=2), backoff=NoWait())
    )
    assert (
        await Processor(settings, producer, Metrics()).handle(worker, [rec({})], lambda: True)
        is False
    )
    (dead,) = sent_to(producer, "dlq")
    assert headers_of(dead)["reason"] == "still locked"


async def test_outcome_count_mismatch_dead_letters_the_whole_call(settings, spec):
    async def handler(messages, *_):
        return [Outcome.ok()]

    producer = Producer()
    worker = replace(
        spec, fn=handler, bulk_mode=True, batch_size=2, retry_to_dlq=header_dlq(max_attempts=3)
    )
    records = [rec({"n": 0}, offset=0), rec({"n": 1}, offset=1)]
    assert (
        await Processor(settings, producer, Metrics()).handle(worker, records, lambda: True)
        is False
    )
    dead = sent_to(producer, "dlq")
    assert len(dead) == 2 and {headers_of(d)["reason"] for d in dead} == {"outcome_count_mismatch"}


async def test_unsendable_output_dead_letters_its_message_with_target_topic(settings, spec):
    async def handler(message, *_):
        return Outcome.ok({"system": {"_id": "d1"}}, label="stored")

    producer = SizeLimitedProducer(refuse_topic="o2")
    worker = replace(
        spec, fn=handler, topics_out=["o1", "o2"], key_by="system._id", retry_to_dlq=header_dlq()
    )
    assert await Processor(settings, producer, Metrics()).handle(worker, [rec({})], lambda: True)
    assert len(sent_to(producer, "o1")) == 1 and sent_to(producer, "o1")[0][2]["key"] == b"d1"
    (dead,) = sent_to(producer, "dlq")
    assert json.loads(dead[1]) == {"system": {"_id": "d1"}}
    assert headers_of(dead)["stage"] == "forward" and headers_of(dead)["target_topic"] == "o2"


async def test_oversized_dead_letter_becomes_a_reference(settings, spec):
    async def handler(message, *_):
        return Outcome.dead_letter("nope")

    producer, metrics = SizeLimitedProducer(limit=1000), Metrics()
    record = rec(b'{"blob": "' + b"x" * 5000 + b'"}')
    worker = replace(spec, fn=handler, retry_to_dlq=header_dlq())
    await Processor(settings, producer, metrics).handle(worker, [record], lambda: True)
    (dead,) = sent_to(producer, "dlq")
    reference = json.loads(dead[1])
    assert reference["_truncated"] is True and reference["size"] == len(record.value)
    assert len(reference["sha256"]) == 64 and headers_of(dead)["reason"] == "nope"
    assert metrics.registry.get_sample_value("qf_dlq_truncated_total", {"worker": "test"}) == 1


# ------------------------------------------------------------ transient
class TransientStoreError(Exception):
    pass


async def test_transient_errors_retry_without_dead_letters_or_attempt_budget(settings, spec):
    calls = 0

    async def handler(message, *_):
        nonlocal calls
        calls += 1
        if calls <= 3:
            raise TransientStoreError("es down")
        return {"id": "ok"}

    waits = NoWait()
    policy = replace(
        header_dlq(max_attempts=1), transient=(TransientStoreError,), transient_backoff=waits
    )
    producer, metrics = Producer(), Metrics()
    assert await Processor(settings, producer, metrics).handle(
        replace(spec, fn=handler, retry_to_dlq=policy), [rec({})], lambda: True
    )
    assert calls == 4 and waits.calls == [0, 1, 2]
    assert not sent_to(producer, "dlq") and len(sent_to(producer, "out")) == 1
    assert (
        metrics.registry.get_sample_value(
            "qf_retries_total", {"worker": "test", "kind": "transient"}
        )
        == 3
    )


async def test_transient_retry_stops_when_the_partition_is_revoked(settings, spec):
    from framework.etl.processing import StaleAssignment

    owned = iter([True, True, False])

    async def handler(message, *_):
        raise TransientStoreError("down")

    policy = replace(header_dlq(), transient=(TransientStoreError,), transient_backoff=NoWait())
    with pytest.raises(StaleAssignment):
        await Processor(settings, Producer(), Metrics()).handle(
            replace(spec, fn=handler, retry_to_dlq=policy), [rec({})], lambda: next(owned)
        )


async def test_ordinary_errors_use_the_configured_backoff(settings, spec):
    async def handler(message, *_):
        raise RuntimeError("boom")

    backoff = NoWait()
    producer = Producer()
    worker = replace(
        spec, fn=handler, retry_to_dlq=replace(header_dlq(max_attempts=3), backoff=backoff)
    )
    assert (
        await Processor(settings, producer, Metrics()).handle(worker, [rec({})], lambda: True)
        is False
    )
    assert backoff.calls == [0, 1]
    assert headers_of(sent_to(producer, "dlq")[0])["reason"] == "RuntimeError: boom"


def test_backoff_strategies():
    delays = [ExponentialBackoff(cap_s=60).delay(n) for n in range(8)]
    for delay, expected in zip(delays, [1, 2, 4, 8, 16, 32, 60, 60]):
        assert expected * 0.8 <= delay <= expected * 1.2
    assert all(0 <= FullJitter(0.5, 4).delay(n) <= 4 for n in range(10))
    with pytest.raises(ValueError):
        ExponentialBackoff(jitter=1.5)


# ---------------------------------------------------------- DLQ formats
def letter(**kwargs):
    defaults = dict(
        worker="w",
        consumer_group="g",
        record=rec({"type": "x"}, offset=42, partition=1, key=b"k1"),
        stage="handler",
        attempts=1,
        error=RuntimeError("boom"),
        value=RAW,
    )
    return DeadLetter(**{**defaults, **kwargs})


def test_header_dead_letter_value_and_headers():
    built = HeaderDeadLetter(worker_header="flow").build(letter())
    headers = {k: v.decode() for k, v in built.headers}
    assert built.value == b'{"type": "x"}' and built.key == b"k1"
    assert headers["reason"] == "RuntimeError: boom" and headers["consumer"] == "g"
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}\+00:00", headers["failed_at"])
    assert headers["flow"] == "w" and headers["stage"] == "handler"
    assert (headers["source_topic"], headers["source_partition"], headers["source_offset"]) == (
        "in",
        "1",
        "42",
    )
    assert headers["source_timestamp_ms"] == "1790000000000"


def test_header_dead_letter_reason_limit_and_explicit_values():
    built = HeaderDeadLetter().build(letter(reason="e" * 5000, value={"a": "ă"}, element_index=2))
    headers = dict(built.headers)
    assert len(headers["reason"]) == 2000 and built.value == b'{"a": "\\u0103"}'
    assert headers["source_element_index"] == b"2"
    assert HeaderDeadLetter().build(letter(value=b"raw")).value == b"raw"


def test_header_dead_letter_truncates_large_values():
    built = HeaderDeadLetter(max_value_bytes=10).build(letter(value={"blob": "x" * 50}))
    assert json.loads(built.value)["_truncated"] is True


def test_diagnostic_envelope_hides_exception_text_and_names_stages_only_when_asked():
    envelope = json.loads(DiagnosticEnvelope().build(letter(error=RuntimeError("secret"))).value)
    assert "secret" not in json.dumps(envelope) and "stage" not in envelope
    staged = json.loads(
        DiagnosticEnvelope().build(letter(reason="not_an_object", stage="decode")).value
    )
    assert staged["stage"] == "decode"


# ------------------------------------------------------------ validation
@pytest.mark.parametrize(
    "change",
    [
        {"auto_offset_reset": "middle"},
        {"consumer_group": " "},
        {"bootstrap_servers": ""},
        {"key_by": "a..b"},
        {"encoder": "json"},
    ],
)
def test_invalid_worker_options_are_refused(spec, change):
    with pytest.raises(ValueError):
        validate_worker(replace(spec, **change))


def test_aggregators_cannot_split_arrays_and_sinks_are_valid(spec):
    validate_worker(replace(spec, topics_out=[]))
    aggregator = replace(spec, kind="aggregator", aggregate_by="id", topics_in=["a", "b"])
    with pytest.raises(ValueError):
        validate_worker(replace(aggregator, split_arrays=True))
    with pytest.raises(ValueError):
        validate_worker(replace(aggregator, topics_out=[]))


def test_retry_to_dlq_validates_new_options():
    with pytest.raises(ValueError):
        retry_to_dlq(dlq_topic="d", transient=("x",))
    with pytest.raises(ValueError):
        retry_to_dlq(dlq_topic="d", dlq_format=object())
    with pytest.raises(ValueError):
        retry_to_dlq(dlq_topic="d", backoff=object())
    assert retry_to_dlq(
        dlq_topic="d", transient=(TransientStoreError,), dlq_format=HeaderDeadLetter()
    )


# ----------------------------------------------------------------- lanes
class TopicConsumer(Consumer):
    def __init__(self, topic, records=()):
        super().__init__(records)
        self.tp = TopicPartition(topic, 0)


async def eventually(predicate, timeout=3):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def test_workers_with_their_own_group_get_their_own_consumer(settings, spec, fake_module):
    seen = []

    async def handler(message, group, metadatas):
        seen.append((group, message["n"]))

    main = replace(spec, name="main", fn=handler)
    other = replace(
        spec,
        name="other",
        topics_in=["side"],
        consumer_group="g2",
        auto_offset_reset="latest",
        fn=handler,
    )
    default_consumer = TopicConsumer("in", [rec({"n": 1}, offset=0)])
    created = {}

    def consumer_factory(lane):
        created[lane.group] = TopicConsumer("side", [rec({"n": 2}, topic="side", offset=5)])
        created["reset"] = lane.offset_reset
        return created[lane.group]

    runtime = KafkaRuntime(
        settings,
        consumer=default_consumer,
        producer=Producer(),
        workers=[main, other],
        consumer_factory=consumer_factory,
    )
    await runtime.start()
    try:
        await eventually(lambda: len(seen) == 2)
        await eventually(lambda: default_consumer.commits and created["g2"].commits)
    finally:
        await runtime.stop()
    assert sorted(seen) == [("g2", 2), ("test", 1)]
    assert created["reset"] == "latest" and len(runtime.lanes) == 2
    assert default_consumer.commits[default_consumer.tp] == 1
    assert created["g2"].commits[created["g2"].tp] == 6
    assert created["g2"].stopped and default_consumer.stopped


async def test_worker_on_another_cluster_publishes_with_that_clusters_producer(
    settings, spec, fake_module
):
    async def handler(message, *_):
        if message.get("bad"):
            raise RuntimeError("bad")
        return {"id": "out"}

    remote = Producer()
    worker = replace(
        spec, name="remote", fn=handler, bootstrap_servers="stt:9092", retry_to_dlq=header_dlq()
    )
    consumer = TopicConsumer("in", [rec({}, offset=0), rec({"bad": True}, offset=1)])
    lanes = []

    def consumer_factory(lane):
        lanes.append(lane.bootstrap)
        return consumer

    local = Producer()
    runtime = KafkaRuntime(
        settings,
        producer=local,
        workers=[worker],
        consumer_factory=consumer_factory,
        producer_factory=lambda bootstrap: remote,
    )
    await runtime.start()
    try:
        await eventually(lambda: consumer.commits.get(consumer.tp) == 2)
    finally:
        await runtime.stop()
    assert lanes == ["stt:9092"]
    assert [s[0] for s in remote.sent] == ["out", "dlq"] and not local.sent
    assert remote.stopped


async def test_disabled_workers_are_not_started(settings, spec, fake_module):
    calls = []
    a = replace(spec, name="a", fn=lambda *x: calls.append("a"))
    b = replace(spec, name="b", topics_in=["other"], fn=lambda *x: calls.append("b"))
    runtime = KafkaRuntime(
        settings.model_copy(update={"disabled_workers": ["b"]}),
        consumer=Consumer([rec({}, offset=0)]),
        producer=Producer(),
        workers=[a, b],
    )
    await runtime.start()
    try:
        await eventually(lambda: calls)
        assert [w.name for w in runtime.workers] == ["a"]
    finally:
        await runtime.stop()
    bad = KafkaRuntime(
        settings.model_copy(update={"disabled_workers": ["nope"]}),
        consumer=Consumer(),
        producer=Producer(),
        workers=[a],
    )
    with pytest.raises(ValueError, match="nope"):
        await bad.start()


def test_worker_spec_defaults_keep_the_2_0_behaviour():
    spec = WorkerSpec(
        kind="handler", name="x", topics_in=["a"], topics_out=["b"], metadatas={}, max_workers=1
    )
    assert (spec.consumer_group, spec.bootstrap_servers, spec.auto_offset_reset) == (
        None,
        None,
        "earliest",
    )
    assert (
        not spec.split_arrays and not spec.with_context and spec.inject_id and spec.key_by == "id"
    )
    assert spec.encoder is None and RetryToDlqConfig().dlq_format is None
