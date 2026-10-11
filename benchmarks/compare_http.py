"""Closed-loop HTTP load test with bounded clients; real TCP, per-request latencies."""

import argparse
import asyncio
import json
import platform
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psutil

ROOT = Path(__file__).resolve().parents[1]


def quantile(values, q):
    return sorted(values)[min(len(values) - 1, int((len(values) - 1) * q))]


async def trial(args, engine, workload, concurrency, repeat):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    python = (
        str(Path(args.original_root) / ".venv/bin/python")
        if engine == "flask_gevent"
        else sys.executable
    )
    command = [
        python,
        str(ROOT / "benchmarks/http_server.py"),
        "--original-root",
        args.original_root,
        "--port",
        str(port),
    ]
    if engine == "flask_gevent":
        command.append("--original")
    log = (
        ROOT / "benchmarks/results" / f"http-{engine}-{workload}-{concurrency}-{repeat}.log"
    ).open("w")
    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
    proc = psutil.Process(process.pid)
    try:
        async with httpx.AsyncClient(
            limits=httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency),
            timeout=15,
        ) as client:
            url = f"http://127.0.0.1:{port}/bench/{workload}"
            async with asyncio.timeout(20):
                while True:
                    if process.poll() is not None:
                        raise RuntimeError(f"Server failed: {log.name}")
                    try:
                        response = await client.post(url, json={"value": 1})
                        if response.status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.05)

            async def drive(n):
                counter, errors, latencies = 0, 0, []

                async def worker():
                    nonlocal counter, errors
                    while counter < n:
                        counter += 1
                        started = time.perf_counter()
                        try:
                            response = await client.post(url, json={"value": 1})
                            if response.status_code != 200 or response.json() != {"value": 1}:
                                errors += 1
                        except Exception:
                            errors += 1
                        latencies.append((time.perf_counter() - started) * 1000)

                started = time.perf_counter()
                await asyncio.gather(*(worker() for _ in range(concurrency)))
                elapsed = time.perf_counter() - started
                return {
                    "requests": n,
                    "errors": errors,
                    "elapsed_sec": elapsed,
                    "requests_per_sec": n / elapsed,
                    "p50_ms": quantile(latencies, 0.5),
                    "p95_ms": quantile(latencies, 0.95),
                    "p99_ms": quantile(latencies, 0.99),
                }

            await drive(args.warmup)
            before = proc.cpu_times()
            rss = []

            async def sample():
                while True:
                    rss.append(proc.memory_info().rss)
                    await asyncio.sleep(0.01)

            sampling = asyncio.create_task(sample())
            try:
                result = await drive(args.requests)
            finally:
                sampling.cancel()
                await asyncio.gather(sampling, return_exceptions=True)
            after = proc.cpu_times()
            result.update(
                engine=engine,
                workload=workload,
                concurrency=concurrency,
                repeat=repeat,
                cpu_sec=(after.user + after.system) - (before.user + before.system),
                peak_rss_mib=max(rss, default=0) / 1024**2,
            )
            return result
    finally:
        process.terminate()
        try:
            await asyncio.to_thread(process.wait, 5)
        except subprocess.TimeoutExpired:
            process.kill()
            await asyncio.to_thread(process.wait)
        log.close()


async def main(args):
    results = []
    for workload in args.workloads.split(","):
        for concurrency in map(int, args.concurrency.split(",")):
            for repeat in range(args.repeats):
                engines = ["flask_gevent", "fastapi_async"]
                if repeat % 2:
                    engines.reverse()
                for engine in engines:
                    result = await trial(args, engine, workload, concurrency, repeat)
                    results.append(result)
                    print(json.dumps(result), flush=True)
                    Path(args.output).write_text(
                        json.dumps(
                            {
                                "environment": {
                                    "platform": platform.platform(),
                                    "python": sys.version,
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
    p.add_argument("--requests", type=int, default=2000)
    p.add_argument("--warmup", type=int, default=128)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--concurrency", default="1,16,64,256")
    p.add_argument("--workloads", default="noop,io")
    p.add_argument("--output", default=str(ROOT / "benchmarks/results/http.json"))
    asyncio.run(main(p.parse_args()))
