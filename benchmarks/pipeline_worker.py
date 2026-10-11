"""Process-isolated worker host. Run through compare_pipelines.py."""

import sys

if "--gevent" in sys.argv:
    from gevent import monkey

    monkey.patch_all()

import argparse
import asyncio
import functools
import logging
import signal
import time
from pathlib import Path
from types import ModuleType

parser = argparse.ArgumentParser()
parser.add_argument(
    "--engine", choices=["original", "original_ack", "async", "sync"], required=True
)
parser.add_argument("--original-root", required=True)
parser.add_argument("--topic", required=True)
parser.add_argument("--group", required=True)
parser.add_argument("--workload", choices=["noop", "io", "bulk", "retry"], required=True)
parser.add_argument("--bootstrap", default="localhost:29092")
parser.add_argument("--gevent", action="store_true")
parser.add_argument("--commit-tick", type=float, default=0.02)
parser.add_argument("--idempotency", action="store_true")
parser.add_argument("--concurrency", type=int, default=8)
args = parser.parse_args()

if args.engine.startswith("original"):
    sys.path.insert(0, str(Path(args.original_root) / "src"))
    module = ModuleType("config")

    class Config:
        WORKER_NAME = args.group
        ERROR_TOPIC = args.topic + "-dlq"
        KAFKA_BOOTSTRAP_SERVERS = args.bootstrap
        REDIS_HOST, REDIS_PORT, REDIS_DB = "localhost", 26379, 0
        KAFKA_POLL_TIMEOUT_MS, KAFKA_IDLE_SLEEP_SEC = 25, 0
        KAFKA_POLL_MAX_RECORDS, KAFKA_PENDING_MAX_PER_TP = 500, 128
        KAFKA_COMMIT_TICK_SEC = args.commit_tick
        KAFKA_PRODUCER_ACKS = "all"
        KAFKA_BULK_OUTPUT_SYNC = True

    module.Config = Config
    sys.modules["config"] = module

from framework.decorators import kafka_handler, retry_to_dlq
from framework.commons.logger import logger

logger.setLevel(logging.ERROR)

seen = set()


def sync_handler(payload, *_):
    if args.workload in {"io", "bulk"}:
        time.sleep(0.005)
    if args.workload == "retry" and payload["n"] % 10 == 0 and payload["id"] not in seen:
        seen.add(payload["id"])
        raise ValueError("transient")
    return payload


async def async_handler(payload, *_):
    if args.workload in {"io", "bulk"}:
        await asyncio.sleep(0.005)
    if args.workload == "retry" and payload["n"] % 10 == 0 and payload["id"] not in seen:
        seen.add(payload["id"])
        raise ValueError("transient")
    return payload


handler = async_handler if args.engine == "async" else sync_handler
handler = retry_to_dlq(max_attempts=3, dlq_topic=args.topic + "-dlq")(handler)
kafka_handler(
    name="bench",
    topics_in=[args.topic],
    topics_out=[args.topic + "-out"],
    max_workers=args.concurrency,
    bulk_mode=args.workload == "bulk",
    batch_size=32,
    batch_timeout_ms=10,
)(handler)

if args.engine.startswith("original"):
    from framework.etl import framework_etl as etl

    if args.engine == "original_ack":
        # Explicit benchmark-only hardening, not presented as stock behavior.
        original = etl._run_handler_single

        @functools.wraps(original)
        def ack_gated(*a, **kw):
            kw["output_sync"] = True
            return original(*a, **kw)

        etl._run_handler_single = ack_gated
    etl.start(worker_modules=[], bootstrap_servers=args.bootstrap, consumer_name=args.group)
else:
    from framework.config import FrameworkSettings
    from framework.etl.framework_etl import KafkaRuntime

    async def main():
        settings = FrameworkSettings(
            worker_modules=["json"],
            kafka_bootstrap_servers=args.bootstrap,
            consumer_name=args.group,
            error_topic=args.topic + "-dlq",
            pending_max_per_tp=128,
            poll_timeout_ms=25,
            poll_max_records=500,
            commit_tick_sec=args.commit_tick,
            idempotency_enabled=args.idempotency,
            redis_url="redis://localhost:26379/0",
            retry_base_sec=0.001,
            retry_max_sec=0.01,
            log_level="ERROR",
        )
        runtime = KafkaRuntime(settings)
        stop = asyncio.Event()
        for signum in [signal.SIGTERM, signal.SIGINT]:
            asyncio.get_running_loop().add_signal_handler(signum, stop.set)
        await runtime.start()
        try:
            await stop.wait()
        finally:
            await runtime.stop()

    asyncio.run(main())
