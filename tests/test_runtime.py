import asyncio
from dataclasses import replace

from framework.etl.framework_etl import KafkaRuntime
from conftest import Consumer, Producer, record, eventually


async def test_bounded_stress_slow_first_record(settings, spec, fake_module):
    release = asyncio.Event()

    async def worker(message, *_):
        if message["n"] == 0:
            await release.wait()
        return message

    records = [record(i, f'{{"n":{i}}}'.encode()) for i in range(300)]
    consumer, producer = Consumer(records), Producer()
    settings.pending_max_per_tp = 16
    runtime = KafkaRuntime(
        settings, consumer=consumer, producer=producer, workers=[replace(spec, fn=worker)]
    )
    await runtime.start()
    try:
        await eventually(lambda: len(producer.sent) >= 10)
        await asyncio.sleep(0.03)
        progress = runtime.partitions[consumer.tp].progress
        assert len(progress.offsets) <= 16
        assert len(runtime.active[spec.name]) <= spec.max_workers
        assert not consumer.commits
        release.set()
        await eventually(lambda: consumer.commits.get(consumer.tp) == 300)
        assert len(producer.sent) == 300
    finally:
        release.set()
        await runtime.stop()
    assert consumer.stopped and producer.stopped


async def test_bulk_flushes_partial_batch_on_timeout(settings, spec, fake_module):
    batches = []

    async def bulk(messages, *_):
        batches.append(len(messages))
        return messages

    runtime = KafkaRuntime(
        settings,
        consumer=Consumer([record()]),
        producer=Producer(),
        workers=[replace(spec, fn=bulk, bulk_mode=True, batch_size=50, batch_timeout_ms=10)],
    )
    await runtime.start()
    try:
        await eventually(lambda: batches)
        assert batches == [1]
    finally:
        await runtime.stop()


async def test_terminal_output_failure_marks_not_ready_and_keeps_offset(
    settings, spec, fake_module
):
    consumer = Consumer([record()])
    runtime = KafkaRuntime(
        settings, consumer=consumer, producer=Producer(fail_topic="out"), workers=[spec]
    )
    await runtime.start()
    try:
        await eventually(lambda: runtime.error is not None)
        assert not runtime.ready
        assert not consumer.commits
    finally:
        await runtime.stop()


async def test_transient_commit_failure_retries_without_loss(settings, spec, fake_module):
    consumer = Consumer([record()], commit_failures=1)
    runtime = KafkaRuntime(settings, consumer=consumer, producer=Producer(), workers=[spec])
    await runtime.start()
    try:
        await eventually(lambda: consumer.commits.get(consumer.tp) == 1)
        assert runtime.error is None
    finally:
        await runtime.stop()


async def test_revoke_cancels_slow_job_and_fences_generation(settings, spec, fake_module):
    started = asyncio.Event()

    async def slow(message, *_):
        started.set()
        await asyncio.sleep(10)
        return message

    consumer, producer = Consumer([record()]), Producer()
    runtime = KafkaRuntime(
        settings, consumer=consumer, producer=producer, workers=[replace(spec, fn=slow)]
    )
    await runtime.start()
    try:
        await started.wait()
        old = runtime.partitions[consumer.tp]
        await runtime.revoke({consumer.tp})
        assert old.revoked and not producer.sent
        assert consumer.tp not in runtime.partitions
    finally:
        await runtime.stop()
