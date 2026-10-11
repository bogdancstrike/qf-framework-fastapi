"""Real SIGKILL/restart E2E. The commit interval is widened to expose the crash window."""

import asyncio
import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition

from test_e2e import broker as broker, BOOTSTRAP, read_outputs

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("QF_INTEGRATION") != "1", reason="Live services required"),
]


@pytest.mark.parametrize("dedup", [False, True])
async def test_sigkill_after_output_ack_before_commit(broker, tmp_path, dedup):
    topics, producer = broker
    root = Path(__file__).resolve().parents[1]
    command = [
        sys.executable,
        str(root / "benchmarks/pipeline_worker.py"),
        "--engine",
        "async",
        "--original-root",
        str(root.parent / "qf_framework"),
        "--topic",
        topics[0],
        "--group",
        topics[0],
        "--workload",
        "noop",
        "--bootstrap",
        BOOTSTRAP,
    ]
    # Host uses <input>-out and <input>-dlq; use those output topics explicitly.
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    await admin.start()
    output_topic = topics[0] + "-out"
    await admin.create_topics([NewTopic(output_topic, 2, 1), NewTopic(topics[0] + "-dlq", 2, 1)])
    if dedup:
        command.append("--idempotency")
    log = (tmp_path / "worker.log").open("w")
    first = second = None
    checker = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP, group_id=topics[0], enable_auto_commit=False
    )
    try:
        await checker.start()
        first = subprocess.Popen(
            command + ["--commit-tick", "60"], stdout=log, stderr=subprocess.STDOUT
        )
        for i in range(20):
            await producer.send_and_wait(
                topics[0], json.dumps({"id": str(i)}).encode(), partition=0
            )
        initial = await read_outputs(output_topic, 20)
        assert len(initial) == 20
        await asyncio.sleep(0.2)  # Let the optional completion ledger finish.
        assert await checker.committed(TopicPartition(topics[0], 0)) is None
        first.send_signal(signal.SIGKILL)
        await asyncio.to_thread(first.wait)
        second = subprocess.Popen(
            command + ["--commit-tick", ".02"], stdout=log, stderr=subprocess.STDOUT
        )
        async with asyncio.timeout(40):
            while (await checker.committed(TopicPartition(topics[0], 0)) or 0) < 20:
                if second.poll() is not None:
                    raise AssertionError((tmp_path / "worker.log").read_text())
                await asyncio.sleep(0.05)
        replayed = await read_outputs(output_topic, 20 if dedup else 40)
        assert {value["id"] for value in replayed} == {str(i) for i in range(20)}
        assert len(replayed) == (20 if dedup else 40)
    finally:
        for process in (first, second):
            if process and process.poll() is None:
                process.terminate()
                try:
                    await asyncio.to_thread(process.wait, 10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    await asyncio.to_thread(process.wait)
        await checker.stop()
        await admin.delete_topics([output_topic, topics[0] + "-dlq"])
        await admin.close()
        log.close()
