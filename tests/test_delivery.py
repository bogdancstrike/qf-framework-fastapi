"""Regressions for partial ACK and ambiguous delivery timeouts."""

import asyncio
import json
from collections import Counter
from dataclasses import replace

from framework.etl.processing import Processor
from framework.observability import Metrics
from conftest import Producer, record


class PartialProducer:
    def __init__(self, failed_topic="b"):
        self.calls = Counter()
        self.sent = []
        self.failed_topic = failed_topic

    async def send(self, topic, value, **kwargs):
        self.calls[topic] += 1
        receipt = asyncio.get_running_loop().create_future()
        if topic == self.failed_topic and self.calls[topic] == 1:
            receipt.set_exception(RuntimeError("injected one delivery failure"))
        else:
            self.sent.append((topic, value))
            receipt.set_result(None)
        return receipt


async def test_acknowledged_fanout_output_is_not_resubmitted(settings, spec):
    producer = PartialProducer()
    await Processor(settings, producer, Metrics()).handle(
        replace(spec, topics_out=["a", "b"]), [record()], lambda: True
    )
    assert producer.calls == {"a": 1, "b": 2}
    assert [topic for topic, _ in producer.sent].count("a") == 1


async def test_late_ack_reuses_original_receipt_without_resubmitting(settings, spec):
    settings.output_ack_timeout_sec = 0.01
    settings.transport_attempts = 6
    producer = Producer(ack_delay=0.025)
    await Processor(settings, producer, Metrics()).handle(spec, [record()], lambda: True)
    assert producer.calls == 1
    assert len(producer.sent) == 1


async def test_partially_acknowledged_bulk_dlq_is_not_resubmitted(settings, spec):
    class PartialDlq(PartialProducer):
        async def send(self, topic, value, **kwargs):
            envelope = json.loads(value)
            return await super().send(str(envelope["source"]["offset"]), value, **kwargs)

    producer = PartialDlq(failed_topic="1")

    async def fail(*args):
        raise ValueError("invalid batch")

    await Processor(settings, producer, Metrics()).handle(
        replace(spec, bulk_mode=True, fn=fail), [record(0), record(1)], lambda: True
    )
    assert producer.calls == {"0": 1, "1": 2}
