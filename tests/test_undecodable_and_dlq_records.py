"""2.2.3: the undecodable-record policy and dead-letter records published as built."""

import json
from dataclasses import dataclass, replace

import pytest

from conftest import Producer
from framework.decorators import kafka_handler
from framework.decorators.intern import RetryToDlqConfig
from framework.decorators.kafka_workers import collect_workers, validate_worker
from framework.etl.dead_letter import DeadLetter, DeadLetterRecord, HeaderDeadLetter
from framework.etl.processing import Processor
from framework.observability import Metrics
from test_importer_parity import header_dlq, rec, sent_to

UNDECODABLE = [b"{bad", b"\xff\xfe", b"5", b'"x"', b"null"]


def dropped(metrics, reason):
    return metrics.registry.get_sample_value(
        "qf_decode_dropped_total", {"worker": "test", "reason": reason}
    )


def skipped(metrics):
    return metrics.registry.get_sample_value(
        "qf_messages_total", {"worker": "test", "outcome": "skipped"}
    )


# ------------------------------------------------------------ on_undecodable
@pytest.mark.parametrize("split_arrays", [False, True])
@pytest.mark.parametrize("raw", UNDECODABLE)
async def test_drop_publishes_nothing_and_counts_a_skip(settings, spec, raw, split_arrays):
    calls = []
    producer, metrics = Producer(), Metrics()
    worker = replace(
        spec,
        fn=lambda *a: calls.append(a),
        split_arrays=split_arrays,
        on_undecodable="drop",
        retry_to_dlq=header_dlq(),
    )
    await Processor(settings, producer, metrics).handle(worker, [rec(raw)], lambda: True)
    assert not calls and not producer.sent
    reason = "invalid_json" if raw in (b"{bad", b"\xff\xfe") else "not_an_object"
    assert dropped(metrics, reason) == 1 and skipped(metrics) == 1


async def test_drop_of_an_array_without_objects(settings, spec):
    producer, metrics = Producer(), Metrics()
    worker = replace(spec, split_arrays=True, on_undecodable="drop", retry_to_dlq=header_dlq())
    await Processor(settings, producer, metrics).handle(worker, [rec([1, "x"])], lambda: True)
    assert not producer.sent and dropped(metrics, "not_an_object") == 1


async def test_drop_keeps_the_decodable_records_of_the_batch(settings, spec):
    seen = []

    async def handler(message, group, metadatas):
        seen.append(message)

    worker = replace(spec, fn=handler, on_undecodable="drop", inject_id=False)
    await Processor(settings, Producer(), Metrics()).handle(
        worker, [rec(b"{bad"), rec({"a": 1}, offset=1)], lambda: True
    )
    assert seen == [{"a": 1}]


async def test_dead_letter_stays_the_default(settings, spec):
    producer = Producer()
    worker = replace(spec, split_arrays=True, retry_to_dlq=header_dlq())
    assert worker.on_undecodable == "dead_letter"
    await Processor(settings, producer, Metrics()).handle(worker, [rec(b"{bad")], lambda: True)
    (dead,) = sent_to(producer, "dlq")
    assert dead[1] == b"{bad"


def test_an_unknown_policy_is_refused(spec):
    with pytest.raises(ValueError, match="on_undecodable"):
        validate_worker(replace(spec, on_undecodable="ignore"))


def test_kafka_handler_declares_the_policy():
    class Owner:
        @kafka_handler(name="drops", topics_in=["in"], topics_out=[], on_undecodable="drop")
        async def drops(self, message, consumer_name, metadatas):
            return None

    (spec,) = collect_workers(Owner())
    assert spec.on_undecodable == "drop"


# ------------------------------------------------- records published as built
@dataclass(frozen=True)
class ExactFormat:
    """A format reproducing another producer's record: JSON value, three headers, no key."""

    trace_headers: bool

    def build(self, letter: DeadLetter) -> DeadLetterRecord:
        value = json.loads(letter.record.value)
        headers = [("reason", f"Failed to process message: {value}".encode())]
        headers.append(("consumer", letter.consumer_group.encode()))
        return DeadLetterRecord(
            json.dumps(value).encode(), None, headers, trace_headers=self.trace_headers
        )


def failing_worker(spec, dlq_format):
    async def handler(message, group, metadatas):
        raise RuntimeError("boom")

    policy = RetryToDlqConfig(max_attempts=1, dlq_topic="dlq", dlq_format=dlq_format)
    return replace(spec, fn=handler, retry_to_dlq=policy)


@pytest.fixture
def traced(monkeypatch):
    """``dlq_headers`` appends a marker header, standing in for the W3C trace headers."""

    def with_trace(record, base=None):
        return [*(base or []), ("traceparent", b"00-trace")]

    monkeypatch.setattr(Processor, "dlq_headers", staticmethod(with_trace))


async def test_a_record_without_trace_headers_is_published_exactly(settings, spec, traced):
    producer = Producer()
    worker = failing_worker(spec, ExactFormat(trace_headers=False))
    await Processor(settings, producer, Metrics()).handle(worker, [rec({"a": 1})], lambda: True)
    (dead,) = sent_to(producer, "dlq")
    assert dead[1] == b'{"a": 1}'
    assert dead[2]["key"] is None
    assert dead[2]["headers"] == [
        ("reason", b"Failed to process message: {'a': 1}"),
        ("consumer", b"test"),
    ]


async def test_trace_headers_are_added_by_default(settings, spec, traced):
    producer = Producer()
    worker = failing_worker(spec, ExactFormat(trace_headers=True))
    await Processor(settings, producer, Metrics()).handle(worker, [rec({"a": 1})], lambda: True)
    (dead,) = sent_to(producer, "dlq")
    assert [name for name, _ in dead[2]["headers"]] == ["reason", "consumer", "traceparent"]


def test_header_dead_letter_keeps_trace_headers():
    assert DeadLetterRecord(b"", None, []).trace_headers is True
    assert HeaderDeadLetter().worker_header == "worker"
