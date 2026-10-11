"""Real Kafka/Redis two-branch join benchmark; one output per correlation.

Use PYTHONPATH to select an archived framework source tree for before/after runs.
Unlike compare_pipelines.py, driver and worker share a process; do not compare
their CPU/RSS figures. Each trial owns unique topics, group and Redis namespace.
"""

import argparse
import asyncio
import json
import os
import platform
import sys
import time
import uuid
from pathlib import Path

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

import framework
from framework.config import FrameworkSettings
from framework.decorators.kafka_workers import WorkerSpec
from framework.etl.framework_etl import KafkaRuntime


async def trial(args, repeat):
    prefix = "qf-join-bench-" + uuid.uuid4().hex[:12]
    a, b, out, dlq = [prefix + suffix for suffix in ("-a", "-b", "-out", "-dlq")]
    admin = AIOKafkaAdminClient(bootstrap_servers=args.bootstrap)
    producer = AIOKafkaProducer(
        bootstrap_servers=args.bootstrap, enable_idempotence=True, linger_ms=5
    )
    output = AIOKafkaConsumer(
        out,
        dlq,
        bootstrap_servers=args.bootstrap,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    async def join(payload, *_):
        return {**payload, "complete": True}

    worker = WorkerSpec(
        kind="aggregator",
        name=prefix,
        topics_in=[a, b],
        topics_out=[out],
        metadatas={},
        max_workers=8,
        aggregate_by="order.id",
        fn=join,
    )
    settings = FrameworkSettings(
        _env_file=None,
        worker_modules=["json"],
        kafka_bootstrap_servers=args.bootstrap,
        consumer_name=prefix,
        error_topic=dlq,
        redis_url=args.redis,
        pending_max_per_tp=128,
        commit_tick_sec=0.02,
        enable_tracing=False,
        idempotency_enabled=False,
        log_level="ERROR",
    )
    runtime = KafkaRuntime(settings, workers=[worker])
    created = False
    try:
        await admin.start()
        await admin.create_topics([NewTopic(topic, 4, 1) for topic in (a, b, out, dlq)])
        created = True
        await producer.start()
        await output.start()
        await runtime.start()

        async def drive(count, phase):
            expected_offsets, values, latencies = {}, [], []
            started = time.perf_counter()

            async def publish():
                futures = []
                for i in range(count):
                    base = {
                        "id": f"{phase}-{i}",
                        "order": {"id": f"{phase}-{i}"},
                        "sent_at": time.perf_counter(),
                    }
                    for topic, field in ((a, "a"), (b, "b")):
                        futures.append(
                            await producer.send(
                                topic, json.dumps({**base, field: i}).encode(), partition=i % 4
                            )
                        )
                for metadata in await asyncio.gather(*futures):
                    expected_offsets[TopicPartition(metadata.topic, metadata.partition)] = (
                        metadata.offset + 1
                    )

            publishing = asyncio.create_task(publish())
            try:
                async with asyncio.timeout(args.timeout):
                    unique = set()
                    while len(unique) < count:
                        if runtime.error:
                            raise RuntimeError("Join runtime failed") from runtime.error
                        if publishing.done():
                            publishing.result()
                        batches = await output.getmany(timeout_ms=25)
                        for batch in batches.values():
                            for record in batch:
                                if record.topic == dlq:
                                    raise RuntimeError("Unexpected join DLQ")
                                value = json.loads(record.value)
                                if not value["id"].startswith(phase + "-"):
                                    continue
                                if value["a"] != value["b"] or not value["complete"]:
                                    raise RuntimeError("Invalid merged branch output")
                                values.append(value["id"])
                                unique.add(value["id"])
                                latencies.append((time.perf_counter() - value["sent_at"]) * 1000)
                    await publishing
                    elapsed = time.perf_counter() - started
                    while True:
                        actual = {
                            tp: await runtime.consumer.committed(tp) for tp in expected_offsets
                        }
                        if all((actual[tp] or 0) >= off for tp, off in expected_offsets.items()):
                            break
                        await asyncio.sleep(0.01)
                    committed = time.perf_counter() - started
                    extra = await output.getmany(timeout_ms=100)
                    for batch in extra.values():
                        for record in batch:
                            if record.topic == dlq:
                                raise RuntimeError("Unexpected join DLQ")
                            value = json.loads(record.value)
                            if value["id"].startswith(phase + "-"):
                                values.append(value["id"])
                    if unique != {f"{phase}-{i}" for i in range(count)}:
                        raise RuntimeError("Unexpected correlation identifiers")
                    return {
                        "correlations": count,
                        "source_records": count * 2,
                        "elapsed_sec": elapsed,
                        "correlations_per_sec": count / elapsed,
                        "commit_elapsed_sec": committed,
                        "commit_correlations_per_sec": count / committed,
                        "p95_ms": sorted(latencies)[int((len(latencies) - 1) * 0.95)],
                        "duplicates": len(values) - len(unique),
                        "missing": count - len(unique),
                        "dlq": 0,
                    }
            finally:
                publishing.cancel()
                await asyncio.gather(publishing, return_exceptions=True)

        await drive(128, "warmup")
        return {"repeat": repeat, **await drive(args.correlations, "measure")}
    finally:
        await runtime.stop()
        await producer.stop()
        await output.stop()
        if created:
            await admin.delete_topics([a, b, out, dlq])
        await admin.close()


async def main(args):
    results = []
    for repeat in range(args.repeats):
        try:
            result = await trial(args, repeat)
        except Exception as exc:
            result = {"repeat": repeat, "error": f"{type(exc).__name__}: {exc}"}
        results.append(result)
        print(json.dumps(result), flush=True)
        Path(args.output).write_text(
            json.dumps(
                {
                    "environment": {
                        "python": sys.version,
                        "platform": platform.platform(),
                        "cpu_count": os.cpu_count(),
                        "framework_source": str(Path(framework.__file__).resolve()),
                    },
                    "parameters": vars(args),
                    "trials": results,
                },
                indent=2,
            )
        )
    if any(
        "error" in result or result.get("duplicates") or result.get("missing") for result in results
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--correlations", type=int, default=10000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--bootstrap", default="localhost:29092")
    parser.add_argument("--redis", default="redis://localhost:26379/0")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.correlations < 1 or args.repeats < 1 or args.timeout <= 0:
        parser.error("Counts and deadline must be positive")
    asyncio.run(main(args))
