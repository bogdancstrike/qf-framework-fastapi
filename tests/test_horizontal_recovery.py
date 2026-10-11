"""Independent replicas: scale, SIGKILL, replace and recover real Kafka/Redis work."""

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from test_e2e import BOOTSTRAP, REDIS

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("QF_INTEGRATION") != "1", reason="Live Kafka/Redis required"),
]
ROOT = Path(__file__).resolve().parents[1]


class Replicas:
    def __init__(self, directory, prefix, kind, ledger=False):
        self.directory, self.prefix, self.kind, self.ledger = directory, prefix, kind, ledger
        self.processes, self.logs = [], []

    def start(self):
        log = (self.directory / f"pod-{len(self.processes)}.log").open("w")
        command = [
            sys.executable,
            str(ROOT / "tests/support/pod_worker.py"),
            "--prefix",
            self.prefix,
            "--directory",
            str(self.directory),
            "--kind",
            self.kind,
            "--bootstrap",
            BOOTSTRAP,
            "--redis",
            REDIS,
        ]
        if self.ledger:
            command.append("--ledger")
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        self.processes.append(process)
        self.logs.append(log)
        return process

    async def until(self, condition, timeout=50):
        async with asyncio.timeout(timeout):
            while True:
                for process, log in zip(self.processes, self.logs):
                    if process.returncode not in (None, -signal.SIGKILL):
                        raise AssertionError(Path(log.name).read_text())
                    if process.poll() not in (None, -signal.SIGKILL):
                        raise AssertionError(Path(log.name).read_text())
                result = condition()
                if result:
                    return result
                await asyncio.sleep(0.025)

    def assignments(self, processes):
        paths = [self.directory / f"status-{p.pid}.json" for p in processes]
        if not all(path.exists() for path in paths):
            return None
        result = [json.loads(path.read_text()) for path in paths]
        owned = [tuple(tp) for snapshot in result for tp in snapshot["assignment"]]
        expected = 8 if self.kind == "join" else 4
        if len(owned) != expected or len(set(owned)) != expected:
            return None
        if not all(snapshot["assignment"] for snapshot in result):
            return None
        return result

    async def close(self):
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
        for process in self.processes:
            if process.poll() is None:
                try:
                    await asyncio.to_thread(process.wait, 8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    await asyncio.to_thread(process.wait)
        for log in self.logs:
            log.close()


@pytest.fixture
async def cluster(tmp_path):
    prefix = "qf-replicas-" + uuid.uuid4().hex[:12]
    topics = [prefix + suffix for suffix in ("-a", "-b", "-out", "-dlq")]
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, enable_idempotence=True)
    output = AIOKafkaConsumer(
        *topics[2:],
        bootstrap_servers=BOOTSTRAP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    checker = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP, group_id=prefix, enable_auto_commit=False
    )
    replicas = []
    await admin.start()
    await admin.create_topics([NewTopic(topic, 4, 1) for topic in topics])
    try:
        await producer.start()
        await output.start()
        await checker.start()
        yield prefix, topics, producer, output, checker, replicas
    finally:
        for group in replicas:
            await group.close()
        await checker.stop()
        await output.stop()
        await producer.stop()
        await admin.delete_topics(topics)
        await admin.close()


async def publish(producer, topics, count):
    receipts = []
    for i in range(count):
        for branch, topic in enumerate(topics):
            payload = {"id": str(i), "order": {"id": str(i)}, f"branch{branch}": i}
            receipts.append(
                await producer.send(topic, json.dumps(payload).encode(), partition=i % 4)
            )
    expected = {}
    for metadata in await asyncio.gather(*receipts):
        expected[TopicPartition(metadata.topic, metadata.partition)] = metadata.offset + 1
    return expected


async def committed(checker, expected):
    async with asyncio.timeout(50):
        while True:
            actual = {tp: await checker.committed(tp) for tp in expected}
            if all((actual[tp] or 0) >= off for tp, off in expected.items()):
                return {f"{tp.topic}:{tp.partition}": value for tp, value in actual.items()}
            await asyncio.sleep(0.025)


async def collect(output, count, join=False):
    values = []
    async with asyncio.timeout(50):
        while len({value["id"] for value in values}) < count:
            batches = await output.getmany(timeout_ms=100)
            for tp, records in batches.items():
                assert not tp.topic.endswith("-dlq"), [r.value for r in records]
                values.extend(json.loads(record.value) for record in records)
    batches = await output.getmany(timeout_ms=300)
    for tp, records in batches.items():
        assert not tp.topic.endswith("-dlq")
        values.extend(json.loads(record.value) for record in records)
    assert {v["id"] for v in values} == {str(i) for i in range(count)}
    if join:
        assert all(v["branch0"] == v["branch1"] for v in values)
    return values


def evidence(scenario, payload):
    # Explicit output location lets CI/users keep artifacts outside the checkout.
    path = Path(
        os.getenv("QF_RECOVERY_RESULTS", ROOT / "benchmarks/results/horizontal_recovery.json")
    )
    document = json.loads(path.read_text()) if path.exists() else {}
    document[scenario] = {
        "scenario": scenario,
        "broker": BOOTSTRAP,
        "partitions_per_topic": 4,
        "deployment": "independent OS processes; local single broker and Redis",
        **payload,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2) + "\n")


@pytest.mark.parametrize(
    "kind,ledger", [("single", False), ("single", True), ("bulk", True), ("join", False)]
)
async def test_scale_kill_replace_during_active_work(cluster, tmp_path, kind, ledger):
    prefix, topics, producer, output, checker, groups = cluster
    replicas = Replicas(tmp_path, prefix, kind, ledger)
    groups.append(replicas)
    first = [replicas.start(), replicas.start()]
    initial = await replicas.until(lambda: replicas.assignments(first))
    count = 256
    expected = await publish(producer, topics[:2] if kind == "join" else topics[:1], count)
    await replicas.until(lambda: list(tmp_path.glob("entered-*")))
    extra = [replicas.start(), replicas.start()]
    all_pods = first + extra
    scaled = await replicas.until(lambda: replicas.assignments(all_pods))
    victim = await replicas.until(
        lambda: next((p for p in all_pods if (tmp_path / f"entered-{p.pid}").exists()), None)
    )
    started = time.perf_counter()
    victim.kill()
    await asyncio.to_thread(victim.wait)
    replacement = replicas.start()
    survivors = [p for p in all_pods if p is not victim] + [replacement]
    replaced = await replicas.until(lambda: replicas.assignments(survivors))
    (tmp_path / "release").touch()
    values = await collect(output, count, join=kind == "join")
    offsets = await committed(checker, expected)
    assert all(p.poll() is None for p in survivors)
    evidence(
        f"scale-kill-{kind}-ledger-{ledger}",
        {
            "input_records": count * (2 if kind == "join" else 1),
            "expected_unique": count,
            "observed_unique": len({v["id"] for v in values}),
            "missing": 0,
            "duplicates": len(values) - count,
            "dlq": 0,
            "initial_replicas": 2,
            "scaled_replicas": 4,
            "killed_pid": victim.pid,
            "replacement_pid": replacement.pid,
            "output_pids": sorted({v["pod"] for v in values}),
            "kill_to_completed_commits_sec": time.perf_counter() - started,
            "initial_assignments": initial,
            "scaled_assignments": scaled,
            "replacement_assignments": replaced,
            "committed_offsets": offsets,
        },
    )


async def test_committed_incomplete_join_survives_total_worker_restart(cluster, tmp_path):
    prefix, topics, producer, output, checker, groups = cluster
    replicas = Replicas(tmp_path, prefix, "join")
    groups.append(replicas)
    first = replicas.start()
    await replicas.until(lambda: replicas.assignments([first]))
    count = 64
    expected_a = await publish(producer, topics[:1], count)
    await committed(checker, expected_a)
    assert not list(tmp_path.glob("entered-*")), "Incomplete join must not execute"
    first.kill()
    await asyncio.to_thread(first.wait)
    replacement = replicas.start()
    await replicas.until(lambda: replicas.assignments([replacement]))
    (tmp_path / "release").touch()
    # Publish branch 1, preserving the application's correlation contract.
    receipts = [
        await producer.send(
            topics[1],
            json.dumps({"id": str(i), "order": {"id": str(i)}, "branch1": i}).encode(),
            partition=i % 4,
        )
        for i in range(count)
    ]
    expected_b = {}
    for metadata in await asyncio.gather(*receipts):
        expected_b[TopicPartition(metadata.topic, metadata.partition)] = metadata.offset + 1
    values = await collect(output, count, join=True)
    offsets = await committed(checker, {**expected_a, **expected_b})
    assert len(values) == count
    evidence(
        "committed-incomplete-join-total-worker-restart",
        {
            "input_records": count * 2,
            "expected_unique": count,
            "observed_unique": count,
            "missing": 0,
            "duplicates": 0,
            "dlq": 0,
            "committed_first_branch_before_kill": True,
            "killed_pid": first.pid,
            "replacement_pid": replacement.pid,
            "committed_offsets": offsets,
            "scope": "All worker processes replaced; Redis remained available with parts retained within TTL",
        },
    )
