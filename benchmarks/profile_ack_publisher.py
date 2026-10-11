"""Isolate ACK bookkeeping CPU cost; this is not a Kafka throughput benchmark.

Run with the desired runtime on PYTHONPATH. Immediate in-memory receipts remove
network/handler/commit costs, making Prometheus label lookup overhead measurable.
Every trial verifies that every publication was counted exactly once.
"""

import argparse
import asyncio
import cProfile
import hashlib
import inspect
import json
import platform
import statistics
import time
from pathlib import Path

from framework.observability import Metrics
from framework.streams.delivery import AckPublisher, Publication


class ImmediateProducer:
    async def send(self, topic, value, **kwargs):
        receipt = asyncio.get_running_loop().create_future()
        receipt.set_result(None)
        return receipt


async def trial(messages, batch_size):
    async def transport(operation, worker):
        return await operation()

    metrics = Metrics()
    publisher = AckPublisher(ImmediateProducer(), metrics, transport, timeout=30)
    started, cpu_started = time.perf_counter(), time.process_time()
    for offset in range(0, messages, batch_size):
        publications = [
            Publication("out", b'{"id":"x"}', b"x", [])
            for _ in range(min(batch_size, messages - offset))
        ]
        await publisher.publish(publications, "profile", "output", lambda: None)
    elapsed, cpu = time.perf_counter() - started, time.process_time() - cpu_started
    acknowledged = metrics.output_acks.labels("profile", "output")._value.get()
    assert acknowledged == messages, (acknowledged, messages)
    return {"elapsed_sec": elapsed, "cpu_sec": cpu, "acknowledged": acknowledged}


async def main(args):
    await trial(min(5000, args.messages), args.batch_size)
    profiler = cProfile.Profile() if args.profile else None
    if profiler:
        profiler.enable()
    trials = [await trial(args.messages, args.batch_size) for _ in range(args.repeats)]
    if profiler:
        profiler.disable()
        profiler.dump_stats(args.profile)
    source = Path(inspect.getfile(AckPublisher))
    result = {
        "scope": "ACK bookkeeping only; immediate in-memory receipts; no Kafka/Redis/handler/commit",
        "python": platform.python_version(),
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "messages_per_trial": args.messages,
        "batch_size": args.batch_size,
        "trials": trials,
        "median_elapsed_sec": statistics.median(row["elapsed_sec"] for row in trials),
        "median_cpu_sec": statistics.median(row["cpu_sec"] for row in trials),
        "profiled": bool(profiler),
    }
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=100000)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--profile", help="Optional cProfile path; timings then include profiler overhead"
    )
    args = parser.parse_args()
    if min(args.messages, args.batch_size, args.repeats) < 1:
        parser.error("messages, batch-size and repeats must be positive")
    asyncio.run(main(args))
