"""Real process fixture; filesystem gates make destructive crash windows explicit."""

import argparse
import asyncio
import json
import os
import signal
from pathlib import Path

from framework.config import FrameworkSettings
from framework.decorators.kafka_workers import WorkerSpec
from framework.etl.framework_etl import KafkaRuntime


async def main(args):
    directory = Path(args.directory)
    pid = os.getpid()

    active = 0

    async def handler(payload, *_):
        nonlocal active
        active += 1
        (directory / f"entered-{pid}").touch()
        try:
            while not (directory / "release").exists():
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.003)
            if isinstance(payload, list):
                return [{**value, "pod": pid} for value in payload]
            return {**payload, "pod": pid}
        finally:
            active -= 1
            if not active:
                (directory / f"entered-{pid}").unlink(missing_ok=True)

    worker = WorkerSpec(
        kind="aggregator" if args.kind == "join" else "handler",
        name="replicated-worker",
        topics_in=[args.prefix + "-a", args.prefix + "-b"]
        if args.kind == "join"
        else [args.prefix + "-a"],
        topics_out=[args.prefix + "-out"],
        fn=handler,
        metadatas={},
        max_workers=4,
        bulk_mode=args.kind == "bulk",
        batch_size=16,
        batch_timeout_ms=10,
        aggregate_by="order.id",
        aggregator_timeout_sec=120,
    )
    settings = FrameworkSettings(
        _env_file=None,
        worker_modules=["json"],
        kafka_bootstrap_servers=args.bootstrap,
        redis_url=args.redis,
        consumer_name=args.prefix,
        error_topic=args.prefix + "-dlq",
        aggregate_lease_sec=3,
        idempotency_enabled=args.ledger,
        pending_max_per_tp=32,
        commit_tick_sec=0.02,
        rebalance_timeout_sec=10,
        shutdown_timeout_sec=2,
        log_level="ERROR",
    )
    runtime = KafkaRuntime(settings, workers=[worker])
    stopping = asyncio.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(signum, stopping.set)
    await runtime.start()
    try:
        while not stopping.is_set():
            if runtime.error:
                raise RuntimeError("Replica failed") from runtime.error
            snapshot = {
                "pid": pid,
                "assignment": sorted(
                    [tp.topic, tp.partition] for tp in runtime.consumer.assignment()
                ),
            }
            temporary = directory / f"status-{pid}.tmp"
            temporary.write_text(json.dumps(snapshot))
            temporary.replace(directory / f"status-{pid}.json")
            try:
                await asyncio.wait_for(stopping.wait(), 0.05)
            except TimeoutError:
                pass
    finally:
        await runtime.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--kind", choices=["single", "bulk", "join"], required=True)
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--redis", required=True)
    parser.add_argument("--ledger", action="store_true")
    asyncio.run(main(parser.parse_args()))
