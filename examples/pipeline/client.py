"""Provision local demo topics and verify HTTP-to-Kafka results (requires .[dev])."""

import argparse
import asyncio
import json
import os
from uuid import uuid4

import httpx
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from . import topics

BOOTSTRAP = os.getenv("QSINT_KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


async def create_topics():
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        await admin.start()
        # One request per topic makes rerunning after partial creation safe.
        for topic in topics.ALL:
            try:
                await admin.create_topics([NewTopic(topic, 4, 1)])
            except TopicAlreadyExistsError:
                pass
        print(json.dumps({"topics": topics.ALL, "partitions": 4, "replication_factor": 1}))
    finally:
        await admin.close()


async def run(url, jobs, timeout):
    consumer = AIOKafkaConsumer(
        topics.OUTPUT,
        bootstrap_servers=BOOTSTRAP,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    text = "  hello async pipeline  "
    expected = {uuid4().hex for _ in range(jobs)}
    received = {}
    try:
        async with asyncio.timeout(timeout):
            await consumer.start()
            async with httpx.AsyncClient(base_url=url, timeout=15) as client:
                for job_id in sorted(expected):
                    response = await client.post("/jobs", json={"id": job_id, "text": text})
                    response.raise_for_status()
                    print(json.dumps({"submission": response.json()}), flush=True)
            while set(received) != expected:
                batches = await consumer.getmany(timeout_ms=100)
                for batch in batches.values():
                    for record in batch:
                        value = json.loads(record.value)
                        job_id = value.get("id")
                        if job_id not in expected:
                            continue  # Earlier runs have different IDs.
                        if job_id in received:
                            raise RuntimeError(f"Duplicate output observed for {job_id}")
                        if (
                            value.get("complete") is not True
                            or value.get("text") != text.strip()
                            or value.get("statistics") != {"words": 3}
                            or value.get("preview") != "HELLO ASYNC PIPELINE"
                        ):
                            raise RuntimeError(f"Unexpected pipeline output: {value}")
                        received[job_id] = value
                        print(json.dumps({"result": value}), flush=True)
            print(json.dumps({"verified": len(received), "topic": topics.OUTPUT}), flush=True)
    finally:
        await consumer.stop()


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("create-topics")
    demo = subcommands.add_parser("run")
    demo.add_argument("--url", default="http://localhost:4000")
    demo.add_argument("--jobs", type=positive_int, default=3)
    demo.add_argument("--timeout", type=positive_int, default=60)
    args = parser.parse_args()
    asyncio.run(
        create_topics()
        if args.command == "create-topics"
        else run(args.url, args.jobs, args.timeout)
    )


if __name__ == "__main__":
    main()
