"""Query real Jaeger storage after crossing four independent worker processes."""
import asyncio
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
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

pytestmark = [pytest.mark.integration, pytest.mark.skipif(os.getenv("QF_JAEGER") != "1", reason="QF_JAEGER=1 requires Kafka, Redis and Jaeger")]


async def test_otlp_trace_is_linked_through_http_fanout_and_join(tmp_path):
    root = Path(__file__).resolve().parents[1]
    prefix = "qf-trace-" + uuid.uuid4().hex[:12]
    topics = [prefix + suffix for suffix in ["-in", "-a", "-b", "-join-a", "-join-b", "-out"]]
    bootstrap = os.getenv("QF_TEST_KAFKA", "localhost:29092")
    jaeger = os.getenv("QF_TEST_JAEGER", "http://localhost:26686")
    otlp = os.getenv("QF_TEST_OTLP", "http://localhost:24317")
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    output = AIOKafkaConsumer(topics[-1], bootstrap_servers=bootstrap, auto_offset_reset="earliest", enable_auto_commit=False)
    processes, logs = [], []
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        port = server.getsockname()[1]
    try:
        await admin.start()
        await admin.create_topics([NewTopic(topic, 1, 1) for topic in topics])
        await output.start()
        for role in ["a", "b", "merge", "ingress"]:
            log = (tmp_path / f"{role}.log").open("w")
            logs.append(log)
            command = [sys.executable, str(root / "benchmarks/tracing_worker.py"),
                "--role", role, "--prefix", prefix, "--port", str(port), "--bootstrap", bootstrap, "--otlp", otlp]
            processes.append(subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT))
        async with httpx.AsyncClient(timeout=5) as client:
            async with asyncio.timeout(30):
                while True:
                    for process in processes:
                        assert process.poll() is None, [p.read_text() for p in tmp_path.glob("*.log")]
                    try:
                        response = await client.get(f"http://localhost:{port}/health/ready")
                        if response.status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(.1)
            response = await client.post(f"http://localhost:{port}/publish", json={"id": prefix, "text": "linked trace"})
            response.raise_for_status()
            trace_id = response.json()["trace_id"]
            async with asyncio.timeout(30):
                while True:
                    records = await output.getmany(timeout_ms=100)
                    messages = [record for batch in records.values() for record in batch]
                    if messages:
                        break
            result = json.loads(messages[0].value)
            assert result["a"] and result["b"] and result["complete"]
            assert dict(messages[0].headers)["traceparent"].decode().split("-")[1] == trace_id
            # Graceful stop invokes exporter force_flush in all four processes.
            for process in processes:
                process.terminate()
            for process in processes:
                await asyncio.to_thread(process.wait, 15)
            async with asyncio.timeout(30):
                while True:
                    response = await client.get(f"{jaeger}/api/traces/{trace_id}")
                    data = response.json().get("data", []) if response.status_code == 200 else []
                    if data and len(data[0]["spans"]) >= 10:
                        break
                    await asyncio.sleep(.2)
            trace_data = data[0]
            spans = trace_data["spans"]
            assert {span["traceID"] for span in spans} == {trace_id}
            services = {process["serviceName"] for process in trace_data["processes"].values()}
            assert services == {"qf-otlp-" + role for role in ["ingress", "a", "b", "merge"]}
            span_ids = {span["spanID"] for span in spans}
            roots = [span for span in spans if not span["references"]]
            assert len(roots) == 1 and roots[0]["operationName"] == "POST /publish"
            links = []
            for span in spans:
                for reference in span["references"]:
                    assert reference["traceID"] == trace_id
                    assert reference["spanID"] in span_ids
                    links.append({"parent": reference["spanID"], "child": span["spanID"]})
            assert len(links) == len(spans) - 1
            path = root / "benchmarks/results/jaeger_trace.json"
            path.write_text(json.dumps(trace_data, indent=2))
            (root / "benchmarks/results/jaeger_verification.json").write_text(json.dumps({
                "trace_id": trace_id, "spans": len(spans), "services": sorted(services),
                "parent_child_links": len(links), "root": roots[0]["operationName"],
                "ui_url": f"{jaeger}/trace/{trace_id}", "final_kafka_trace_id": trace_id,
                "operations": [span["operationName"] for span in spans]}, indent=2))
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                await asyncio.to_thread(process.wait)
        for log in logs:
            log.close()
        await output.stop()
        try:
            await admin.delete_topics(topics)
        finally:
            await admin.close()
