"""Real broker closed-workload pipeline benchmark; emits all repetitions as JSON."""

import argparse
import asyncio
import json
import os
import platform
import subprocess
import sys
import time
import uuid
from pathlib import Path

import psutil
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]


def percentile(values, q):
    return sorted(values)[min(len(values) - 1, int((len(values) - 1) * q))] if values else None


async def trial(args, engine, workload, repeat):
    topic = "qf-bench-" + uuid.uuid4().hex[:12]
    group = topic + "-group"
    admin = AIOKafkaAdminClient(bootstrap_servers=args.bootstrap)
    producer = AIOKafkaProducer(
        bootstrap_servers=args.bootstrap, enable_idempotence=True, linger_ms=5
    )
    output = AIOKafkaConsumer(
        topic + "-out",
        topic + "-dlq",
        bootstrap_servers=args.bootstrap,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    offsets = AIOKafkaConsumer(
        bootstrap_servers=args.bootstrap, group_id=group, enable_auto_commit=False
    )
    process = None
    log = (ROOT / "benchmarks/results" / f"{engine}-{workload}-{repeat}.log").open("w")
    try:
        await admin.start()
        await admin.create_topics(
            [NewTopic(t, args.partitions, 1) for t in [topic, topic + "-out", topic + "-dlq"]]
        )
        await producer.start()
        await output.start()
        await offsets.start()
        old = engine.startswith("original")
        python = str(Path(args.original_root) / ".venv/bin/python") if old else sys.executable
        command = [
            python,
            str(ROOT / "benchmarks/pipeline_worker.py"),
            "--engine",
            engine,
            "--original-root",
            args.original_root,
            "--topic",
            topic,
            "--group",
            group,
            "--workload",
            workload,
            "--bootstrap",
            args.bootstrap,
            "--concurrency",
            str(args.concurrency),
        ]
        if old:
            command.append("--gevent")
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        proc = psutil.Process(process.pid)

        async def drive(n, phase):
            expected = {}
            started = time.perf_counter()

            async def publish():
                futures = []
                for i in range(n):
                    value = {
                        "id": f"{phase}-{i}",
                        "n": i,
                        "sent_at": time.perf_counter(),
                        "text": "x" * 256,
                    }
                    futures.append(
                        await producer.send(
                            topic, json.dumps(value).encode(), partition=i % args.partitions
                        )
                    )
                for metadata in await asyncio.gather(*futures):
                    expected[TopicPartition(metadata.topic, metadata.partition)] = (
                        metadata.offset + 1
                    )

            publishing = asyncio.create_task(publish())
            latencies, identifiers, dlq = [], [], 0
            async with asyncio.timeout(args.timeout):
                while len(set(identifiers)) < n:
                    records = await output.getmany(timeout_ms=50)
                    now = time.perf_counter()
                    if process.poll() is not None:
                        raise RuntimeError(f"Worker exited; inspect {log.name}")
                    for batch in records.values():
                        for record in batch:
                            value = json.loads(record.value)
                            if record.topic.endswith("-dlq"):
                                dlq += 1
                                continue
                            if value["id"].startswith(phase + "-"):
                                identifiers.append(value["id"])
                                latencies.append((now - value["sent_at"]) * 1000)
                await publishing
                output_elapsed = time.perf_counter() - started
                while True:
                    actual = {tp: await offsets.committed(tp) for tp in expected}
                    if all(
                        actual[tp] is not None and actual[tp] >= off for tp, off in expected.items()
                    ):
                        break
                    await asyncio.sleep(0.01)
            commit_elapsed = time.perf_counter() - started
            extra = await output.getmany(timeout_ms=100)
            duplicates = len(identifiers) - len(set(identifiers))
            for batch in extra.values():
                for record in batch:
                    value = json.loads(record.value)
                    if value.get("id", "").startswith(phase + "-"):
                        duplicates += 1
            return {
                "messages": n,
                "elapsed_sec": output_elapsed,
                "messages_per_sec": n / output_elapsed,
                "commit_elapsed_sec": commit_elapsed,
                "commit_messages_per_sec": n / commit_elapsed,
                "p50_ms": percentile(latencies, 0.5),
                "p95_ms": percentile(latencies, 0.95),
                "p99_ms": percentile(latencies, 0.99),
                "duplicates": duplicates,
                "missing": n - len(set(identifiers)),
                "dlq": dlq,
            }

        await drive(args.warmup, "warmup")
        before = proc.cpu_times()
        rss = []

        async def sample():
            while True:
                rss.append(proc.memory_info().rss)
                await asyncio.sleep(0.01)

        sampling = asyncio.create_task(sample())
        try:
            result = await drive(args.messages, "measure")
        finally:
            sampling.cancel()
            await asyncio.gather(sampling, return_exceptions=True)
        after = proc.cpu_times()
        result.update(
            engine=engine,
            workload=workload,
            repeat=repeat,
            cpu_sec=(after.user + after.system) - (before.user + before.system),
            peak_rss_mib=max(rss, default=proc.memory_info().rss) / 1024**2,
        )
        return result
    finally:
        if process and process.poll() is None:
            process.terminate()
            try:
                await asyncio.to_thread(process.wait, 10)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait)
        log.close()
        await producer.stop()
        await output.stop()
        await offsets.stop()
        try:
            await admin.delete_topics([topic, topic + "-out", topic + "-dlq"])
        finally:
            await admin.close()


async def main(args):
    results = []
    for workload in args.workloads.split(","):
        for repeat in range(args.repeats):
            # Alternate trial order across repetitions to reduce warm-cache/order bias.
            engines = args.engines.split(",")
            if repeat % 2:
                engines.reverse()
            for engine in engines:
                try:
                    result = await trial(args, engine, workload, repeat)
                except Exception as exc:
                    result = {
                        "engine": engine,
                        "workload": workload,
                        "repeat": repeat,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                results.append(result)
                print(json.dumps(result), flush=True)
                Path(args.output).write_text(
                    json.dumps(
                        {
                            "environment": {
                                "python": sys.version,
                                "platform": platform.platform(),
                                "cpu_count": os.cpu_count(),
                                "cpu_affinity": psutil.Process().cpu_affinity(),
                            },
                            "parameters": vars(args),
                            "trials": results,
                        },
                        indent=2,
                    )
                )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--original-root", default=str(ROOT.parent / "qf_framework"))
    p.add_argument("--bootstrap", default="localhost:29092")
    p.add_argument("--engines", default="original,original_ack,async,sync")
    p.add_argument("--workloads", default="noop,io,bulk,retry")
    p.add_argument("--messages", type=int, default=2000)
    p.add_argument("--warmup", type=int, default=128)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--partitions", type=int, default=4)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--timeout", type=float, default=60)
    p.add_argument("--output", default=str(ROOT / "benchmarks/results/pipelines.json"))
    asyncio.run(main(p.parse_args()))
