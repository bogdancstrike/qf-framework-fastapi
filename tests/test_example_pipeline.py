"""Execute the documented example commands against isolated live services."""

import asyncio
import base64
import json
import os
import socket
import subprocess
import sys
import uuid
from pathlib import Path

import httpx
import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("QF_INTEGRATION") != "1", reason="Requires live Kafka and Redis"),
]


async def test_documented_pipeline_cli_and_http_contract(tmp_path):
    root = Path(__file__).resolve().parents[1]
    prefix = "qf-demo-test-" + uuid.uuid4().hex[:12]
    env = {
        **os.environ,
        "QF_DEMO_PREFIX": prefix,
        "QSINT_KAFKA_BOOTSTRAP_SERVERS": os.getenv("QF_TEST_KAFKA", "localhost:29092"),
        "QSINT_REDIS_URL": os.getenv("QF_TEST_REDIS", "redis://localhost:26379/0"),
        "QSINT_CONSUMER_NAME": prefix + ".workers",
        "QSINT_ENABLE_TRACING": "false",
        "QSINT_SHUTDOWN_TIMEOUT_SEC": "5",
        "QSINT_RETRY_BASE_SEC": "0.01",
        "QSINT_LOG_LEVEL": "WARNING",
    }
    admin = AIOKafkaAdminClient(bootstrap_servers=env["QSINT_KAFKA_BOOTSTRAP_SERVERS"])
    process = None
    provisioned = []
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    log_path = tmp_path / "pipeline.log"

    async def cli(*args):
        result = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "-m", "examples.pipeline.client", *args],
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert result.returncode == 0, result.stdout + result.stderr + log_path.read_text()
        return [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]

    with log_path.open("w") as log:
        try:
            await admin.start()
            provisioned = (await cli("create-topics"))[0]["topics"]
            assert (await cli("create-topics"))[0]["topics"] == provisioned
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "examples.pipeline.app:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                ],
                cwd=root,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            url = f"http://127.0.0.1:{port}"
            async with httpx.AsyncClient(base_url=url, timeout=2) as client:
                async with asyncio.timeout(45):
                    while True:
                        assert process.poll() is None, log_path.read_text()
                        try:
                            if (await client.get("/health/ready")).status_code == 200:
                                break
                        except httpx.TransportError:
                            pass
                        await asyncio.sleep(0.1)
                assert (await client.post("/jobs", json={"text": ""})).status_code == 422
                assert (await client.post("/jobs", json={})).status_code == 422
                assert (await client.get("/metrics")).status_code == 200
                rejected = await client.post(
                    "/jobs", json={"id": prefix + "-invalid", "text": "   "}
                )
                assert rejected.status_code == 202
            result = await cli("run", "--url", url, "--jobs", "5", "--timeout", "45")
            submissions = [row["submission"] for row in result if "submission" in row]
            assert len(submissions) == 5
            assert all(row["accepted"] and row["trace_id"] is None for row in submissions)
            assert result[-1] == {"verified": 5, "topic": prefix + ".jobs.done"}
            assert len({row["result"]["id"] for row in result if "result" in row}) == 5
            # A second CLI run must ignore the first run's historical output.
            assert (await cli("run", "--url", url, "--jobs", "1"))[-1]["verified"] == 1
            dead_letters = AIOKafkaConsumer(
                prefix + ".dlq",
                bootstrap_servers=env["QSINT_KAFKA_BOOTSTRAP_SERVERS"],
                auto_offset_reset="earliest",
                enable_auto_commit=False,
            )
            try:
                await dead_letters.start()
                async with asyncio.timeout(15):
                    envelope = json.loads((await dead_letters.getone()).value)
                assert envelope["attempts"] == 3
                assert envelope["error"]["type"] == "ValueError"
                assert envelope["source"]["topic"] == prefix + ".jobs.in"
                assert json.loads(base64.b64decode(envelope["raw_base64"])) == {
                    "id": prefix + "-invalid",
                    "text": "   ",
                }
            finally:
                await dead_letters.stop()
        finally:
            if process:
                process.terminate()
                try:
                    await asyncio.to_thread(process.wait, timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    await asyncio.to_thread(process.wait, timeout=5)
            if provisioned:
                await admin.delete_topics(provisioned)
            await admin.close()
