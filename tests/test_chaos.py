"""Explicit opt-in broker outage test; pauses only the dedicated test Kafka container."""

import asyncio
import json
import os
import subprocess
from dataclasses import replace

import pytest
from aiokafka import AIOKafkaConsumer

from framework.etl.framework_etl import KafkaRuntime
from test_e2e import broker as broker, BOOTSTRAP, runtime_settings

pytestmark = [
    pytest.mark.integration,
    pytest.mark.chaos,
    pytest.mark.skipif(
        os.getenv("QF_CHAOS") != "1", reason="QF_CHAOS=1 permits pausing the dedicated test broker"
    ),
]


async def test_broker_outage_exhaustion_and_restart(broker, spec):
    topics, producer = broker
    gate, entered = asyncio.Event(), asyncio.Event()

    async def handler(payload, *_):
        entered.set()
        await gate.wait()
        return payload

    worker = replace(spec, fn=handler, topics_in=[topics[0]], topics_out=[topics[2]])
    settings = runtime_settings(
        topics,
        output_ack_timeout_sec=0.15,
        transport_attempts=2,
        commit_timeout_sec=0.2,
        shutdown_timeout_sec=2,
    )
    runtime = KafkaRuntime(settings, workers=[worker])
    second = None
    paused = False
    output = AIOKafkaConsumer(
        topics[2],
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    try:
        await output.start()
        await runtime.start()
        for i in range(20):
            await producer.send_and_wait(
                topics[0], json.dumps({"id": str(i)}).encode(), partition=0
            )
        await asyncio.wait_for(entered.wait(), 20)
        await asyncio.to_thread(
            subprocess.run,
            ["docker", "pause", "qf-fastapi-test-kafka-1"],
            check=True,
            capture_output=True,
        )
        paused = True
        gate.set()
        async with asyncio.timeout(5):
            while runtime.error is None:
                await asyncio.sleep(0.01)
        assert not runtime.ready
        await asyncio.to_thread(
            subprocess.run,
            ["docker", "unpause", "qf-fastapi-test-kafka-1"],
            check=True,
            capture_output=True,
        )
        paused = False
        await runtime.stop()
        second = KafkaRuntime(settings, workers=[worker])
        await second.start()
        identifiers = []
        async with asyncio.timeout(40):
            while len(set(identifiers)) < 20:
                batches = await output.getmany(timeout_ms=100)
                identifiers.extend(
                    json.loads(r.value)["id"] for batch in batches.values() for r in batch
                )
        assert set(identifiers) == {str(i) for i in range(20)}
        assert second.error is None
    finally:
        if paused:
            await asyncio.to_thread(
                subprocess.run,
                ["docker", "unpause", "qf-fastapi-test-kafka-1"],
                capture_output=True,
            )
        await runtime.stop()
        if second:
            await second.stop()
        await output.stop()
