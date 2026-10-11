"""Five independent 5,000-message runs through five real Kafka worker stages.

This validates complete processing and retry behavior, not broker failover or
exactly-once business effects. Counts include every intermediate topic and source
commit, rather than checking only the final number of messages received.
"""

import asyncio
import hashlib
import json
import os
import platform
import time
import uuid
from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from framework.config import FrameworkSettings
from framework.etl.framework_etl import KafkaRuntime
from framework.observability import Metrics
from test_e2e import BOOTSTRAP, REDIS

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("QF_INTEGRATION") != "1", reason="Requires live Kafka/Redis"),
]
RESULTS = []


@pytest.mark.parametrize("trial", range(1, 6))
async def test_five_stage_pipeline_processes_every_message_with_retries(spec, trial):
    count, stages, partitions = 5000, 5, 4
    prefix = "qf-pipeline-proof-" + uuid.uuid4().hex[:12]
    topics = [f"{prefix}-{n}" for n in range(stages + 1)]
    dlq = prefix + "-dlq"
    ledger = trial in {2, 4, 5}
    bulk_stage = 2 if trial == 4 else None
    attempts = [Counter() for _ in range(stages)]
    completed = [set() for _ in range(stages)]
    failures, transport_failures = Counter(), set()
    restart_gate = asyncio.Event()
    metrics = Metrics()

    def handler(stage):
        async def process(payload, *_):
            rows = payload if isinstance(payload, list) else [payload]
            identity = tuple(row["id"] for row in rows)
            attempts[stage][identity] += 1
            # Fixed identifiers make the retry load reproducible. The first two
            # attempts fail, and the third succeeds, exercising the full budget.
            if any(int(row["id"]) % 41 == 0 for row in rows) and attempts[stage][identity] <= 2:
                failures[stage] += 1
                raise ValueError("injected transient handler failure")
            await asyncio.sleep(0.0005)
            for row in rows:
                assert row["stages"] == list(range(stage))
                row["stages"].append(stage)
                completed[stage].add(row["id"])
            if stage == stages - 1 and len(completed[stage]) >= 500:
                restart_gate.set()
            return payload

        return process

    workers = [
        replace(
            spec,
            name=f"stage-{stage}",
            topics_in=[topics[stage]],
            topics_out=[topics[stage + 1]],
            max_workers=8,
            fn=handler(stage),
            bulk_mode=stage == bulk_stage,
            batch_size=32 if stage == bulk_stage else 1,
            batch_timeout_ms=10,
        )
        for stage in range(stages)
    ]
    settings = FrameworkSettings(
        _env_file=None,
        worker_modules=["json"],
        kafka_bootstrap_servers=BOOTSTRAP,
        consumer_name=prefix,
        error_topic=dlq,
        redis_url=REDIS,
        idempotency_enabled=ledger,
        pending_max_per_tp=32,
        commit_tick_sec=0.02,
        retry_attempts=3,
        retry_base_sec=0.001,
        retry_max_sec=0.01,
        shutdown_timeout_sec=10,
    )

    class FaultInjectingProducer:
        """Fail known unsent operations; delegate actual ACKs to real Kafka."""

        def __init__(self):
            self.producer = AIOKafkaProducer(
                bootstrap_servers=BOOTSTRAP, enable_idempotence=True, acks="all", linger_ms=5
            )

        async def start(self):
            await self.producer.start()

        async def send(self, topic, value, **kwargs):
            identity = json.loads(value)["id"]
            if (
                topic == topics[3]
                and int(identity) % 97 == 0
                and identity not in transport_failures
            ):
                transport_failures.add(identity)
                raise ConnectionError("injected failure before producer enqueue")
            return await self.producer.send(topic, value, **kwargs)

        async def stop(self):
            await self.producer.stop()

    def make_runtime():
        return KafkaRuntime(
            settings, workers=workers, metrics=metrics, producer=FaultInjectingProducer()
        )

    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    ingress = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, enable_idempotence=True, linger_ms=5)
    output = AIOKafkaConsumer(
        topics[-1],
        dlq,
        bootstrap_servers=BOOTSTRAP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    runtime, publishing, restarting, created = make_runtime(), None, None, False
    started = time.perf_counter()
    try:
        await admin.start()
        await admin.create_topics([NewTopic(t, partitions, 1) for t in [*topics, dlq]])
        created = True
        await ingress.start()
        await output.start()
        await runtime.start()

        async def publish():
            receipts = [
                await ingress.send(
                    topics[0],
                    json.dumps({"id": str(n), "stages": []}).encode(),
                    key=str(n).encode(),
                    partition=n % partitions,
                )
                for n in range(count)
            ]
            await asyncio.gather(*receipts)

        async def restart():
            nonlocal runtime
            await restart_gate.wait()
            await runtime.stop()  # Drain ACKed work; leave unread input for the new runtime.
            runtime = make_runtime()
            await runtime.start()

        publishing = asyncio.create_task(publish())
        if trial == 5:
            restarting = asyncio.create_task(restart())
        seen = []
        async with asyncio.timeout(180):
            while len(set(seen)) < count:
                if runtime.error:
                    raise RuntimeError("Pipeline failed") from runtime.error
                if publishing.done():
                    publishing.result()
                if restarting is not None and restarting.done():
                    restarting.result()
                batches = await output.getmany(timeout_ms=50)
                for tp, batch in batches.items():
                    assert tp.topic != dlq, (
                        "Transient failures must recover within the retry budget"
                    )
                    for row in batch:
                        value = json.loads(row.value)
                        assert value["stages"] == list(range(stages))
                        seen.append(value["id"])
            await publishing
            if restarting:
                await restarting
            output_elapsed = time.perf_counter() - started
            all_tps = [TopicPartition(t, p) for t in [*topics, dlq] for p in range(partitions)]
            ends = await output.end_offsets(all_tps)
            expected = {tp: offset for tp, offset in ends.items() if tp.topic in topics[:-1]}
            while True:
                actual = {tp: await runtime.consumer.committed(tp) for tp in expected}
                if all(actual[tp] == offset for tp, offset in expected.items()):
                    break
                await asyncio.sleep(0.02)
            extra = await output.getmany(timeout_ms=200)
            for tp, batch in extra.items():
                assert tp.topic != dlq or not batch
                seen.extend(json.loads(row.value)["id"] for row in batch)
            ends = await output.end_offsets(all_tps)

        identifiers = {str(n) for n in range(count)}
        assert set(seen) == identifiers and len(seen) == count
        assert all(done == identifiers for done in completed)
        topic_counts = {
            t: sum(ends[TopicPartition(t, p)] for p in range(partitions)) for t in topics
        }
        assert set(topic_counts.values()) == {count}, topic_counts
        assert sum(ends[TopicPartition(dlq, p)] for p in range(partitions)) == 0
        for stage in range(stages):
            assert failures[stage] > 0
            assert (
                metrics.retries.labels(f"stage-{stage}", "handler")._value.get() == failures[stage]
            )
        assert metrics.retries.labels("stage-2", "transport")._value.get() == len(
            transport_failures
        )
        assert runtime.error is None
        result = {
            "trial": trial,
            "messages": count,
            "workers": stages,
            "idempotency_enabled": ledger,
            "bulk_stage": bulk_stage,
            "graceful_runtime_restarts": int(trial == 5),
            "elapsed_to_final_output_sec_including_startup": output_elapsed,
            "elapsed_through_verification_sec": time.perf_counter() - started,
            "processed_unique_per_stage": [len(done) for done in completed],
            "handler_retries_per_stage": [failures[stage] for stage in range(stages)],
            "transport_retries": len(transport_failures),
            "committed_partitions": len(expected),
            "topic_counts": topic_counts,
            "final_outputs": len(seen),
            "missing": 0,
            "duplicates": 0,
            "dlq": 0,
        }
        RESULTS.append(result)
        root = Path(__file__).resolve().parents[1]
        artifact = Path(
            os.getenv(
                "QF_PIPELINE_RESULTS", str(root / "benchmarks/results/pipeline_reliability.json")
            )
        )
        artifact.parent.mkdir(parents=True, exist_ok=True)
        sources = [
            "etl/framework_etl.py",
            "etl/processing.py",
            "etl/policies.py",
            "redis/aggregation.py",
            "streams/delivery.py",
        ]
        artifact.write_text(
            json.dumps(
                {
                    "python": platform.python_version(),
                    "requested_trials": 5,
                    "scope": "real Kafka RF1/Redis; five stages in one process; deterministic transient handler and pre-enqueue transport failures",
                    "source_sha256": {
                        name: hashlib.sha256(
                            (root / "src/framework" / name).read_bytes()
                        ).hexdigest()
                        for name in sources
                    },
                    "trials": RESULTS,
                },
                indent=2,
            )
            + "\n"
        )
    finally:
        for task in (publishing, restarting):
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await runtime.stop()
        await ingress.stop()
        await output.stop()
        if created:
            await admin.delete_topics([*topics, dlq])
        await admin.close()
