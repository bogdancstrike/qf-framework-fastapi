import asyncio
import json
from dataclasses import replace

import pytest

from framework.etl.processing import Processor, DeliveryError, StaleAssignment
from framework.decorators.intern import RetryToDlqConfig
from framework.observability import Metrics
from conftest import Producer, record


async def test_waits_for_output_ack(settings, spec):
    producer = Producer(ack_delay=0.03)
    processor = Processor(settings, producer, Metrics())
    task = asyncio.create_task(processor.handle(spec, [record()], lambda: True))
    await asyncio.sleep(0.005)
    assert not task.done()
    await task
    assert len(producer.sent) == 1


async def test_transport_retry_does_not_reinvoke_worker(settings, spec):
    calls = 0

    async def handler(message, *_):
        nonlocal calls
        calls += 1
        return message

    producer = Producer(failures=1)
    processor = Processor(settings, producer, Metrics())
    await processor.handle(replace(spec, fn=handler), [record()], lambda: True)
    assert calls == 1 and producer.calls == 2


async def test_handler_retry_receives_original_unmutated_input(settings, spec):
    seen = []

    async def handler(message, *_):
        seen.append(dict(message))
        message["dirty"] = True
        if len(seen) < 3:
            raise ValueError("temporary")
        return message

    producer = Producer()
    await Processor(settings, producer, Metrics()).handle(
        replace(spec, fn=handler), [record()], lambda: True
    )
    assert len(seen) == 3
    assert all("dirty" not in item for item in seen)
    assert producer.sent[0][0] == "out"


@pytest.mark.parametrize("value", [b"{", b"null", b"[]", b"4", b'"x"', b"\xff", None])
async def test_malformed_input_preserved_in_dlq(settings, spec, value):
    producer = Producer()
    await Processor(settings, producer, Metrics()).handle(spec, [record(value=value)], lambda: True)
    topic, value, _ = producer.sent[0]
    envelope = json.loads(value)
    assert topic == settings.error_topic
    assert envelope["source"]["offset"] == 0 and "raw_base64" in envelope


async def test_dlq_delivery_failure_is_fatal(settings, spec):
    producer = Producer(fail_topic=settings.error_topic)
    with pytest.raises(DeliveryError):
        await Processor(settings, producer, Metrics()).handle(
            spec, [record(value=b"{")], lambda: True
        )


async def test_output_delivery_failure_is_fatal_and_not_application_dlq(settings, spec):
    producer = Producer(fail_topic="out")
    with pytest.raises(DeliveryError):
        await Processor(settings, producer, Metrics()).handle(spec, [record()], lambda: True)
    assert not producer.sent


async def test_exhausted_handler_goes_to_configured_dlq(settings, spec):
    calls = 0

    async def fail(*args):
        nonlocal calls
        calls += 1
        raise ValueError("sensitive exception body")

    producer = Producer()
    configured = replace(
        spec, fn=fail, retry_to_dlq=RetryToDlqConfig(max_attempts=2, dlq_topic="dlq")
    )
    assert (
        await Processor(settings, producer, Metrics()).handle(configured, [record()], lambda: True)
        is False
    )
    assert calls == 2 and producer.sent[0][0] == "dlq"
    assert b"sensitive" not in producer.sent[0][1]


async def test_revoked_work_cannot_publish(settings, spec):
    producer = Producer()
    with pytest.raises(StaleAssignment):
        await Processor(settings, producer, Metrics()).handle(spec, [record()], lambda: False)
    assert not producer.sent


async def test_none_filters_intentionally(settings, spec):
    async def filter_message(*args):
        return None

    producer = Producer()
    await Processor(settings, producer, Metrics()).handle(
        replace(spec, fn=filter_message), [record()], lambda: True
    )
    assert not producer.sent


async def test_sync_worker_runs_off_loop(settings, spec):
    import threading

    thread = threading.get_ident()

    def worker(message, *_):
        assert threading.get_ident() != thread
        return message

    await Processor(settings, Producer(), Metrics()).handle(
        replace(spec, fn=worker), [record()], lambda: True
    )


async def test_missing_aggregation_key_goes_to_dlq(settings, spec):
    producer = Producer()
    aggregator = replace(spec, kind="aggregator", aggregate_by="correlation")
    processor = Processor(settings, producer, Metrics())
    await processor.handle(aggregator, [record()], lambda: True)
    assert producer.sent[0][0] == settings.error_topic


@pytest.mark.parametrize("identifier", [0, False, "0"])
async def test_falsy_valid_ids_survive_processing(settings, spec, identifier):
    producer = Producer()
    await Processor(settings, producer, Metrics()).handle(
        spec, [record(value=json.dumps({"id": identifier}).encode())], lambda: True
    )
    assert json.loads(producer.sent[0][1])["id"] == identifier
    from framework.decorators.kafka_workers import ensure_message_id

    payload = {"id": identifier}
    assert ensure_message_id(payload) == str(identifier)
    assert payload["id"] == identifier
