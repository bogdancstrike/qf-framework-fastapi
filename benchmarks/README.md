# Tests and reproducible benchmarks

Install `pip install -e '.[dev,tracing]'`. Install the original framework's dependencies and gevent in its separate `.venv`; both distributions use `framework` imports. Scripts launch each implementation in an isolated process and create/delete UUID-scoped Kafka topics. Run benchmarks sequentially, without chaos tests or competing load.

```bash
docker compose -p qf-fastapi-test up -d --wait
python -m pytest -q
QF_INTEGRATION=1 python -m pytest tests/test_e2e.py tests/test_crash_recovery.py -v
QF_JAEGER=1 python -m pytest tests/test_otlp_jaeger.py -v
QF_INTEGRATION=1 QF_CHAOS=1 python -m pytest tests/test_chaos.py -v
QF_INTEGRATION=1 QF_STRESS_MESSAGES=10000 python -m pytest tests/test_e2e.py -k stress -v
```

The chaos test deliberately pauses only `qf-fastapi-test-kafka-1` and unpauses it in cleanup. Use the dedicated stack and run it separately from other consumers/load tests. The full validation command is:

```bash
QF_INTEGRATION=1 QF_CHAOS=1 QF_JAEGER=1 QF_STRESS_MESSAGES=10000 python -m pytest -q
ruff check .
ruff format --check .
python -m build
```

## Matched worker benchmarks

```bash
python benchmarks/compare_pipelines.py --messages 1000 --repeats 3 --concurrency 8
python benchmarks/compare_pipelines.py --messages 5000 --repeats 3 --concurrency 32 \
  --engines original_ack,async,sync --workloads io,bulk \
  --output benchmarks/results/pipelines_scale_rerun.json
python benchmarks/compare_pipelines.py --messages 5000 --repeats 3 --concurrency 32 \
  --engines async,sync,original_ack --workloads bulk \
  --output benchmarks/results/pipelines_bulk_tuned_rerun.json
```

`--original-root` defaults to the sibling `qf_framework`; `--bootstrap` defaults to `localhost:29092`. Default warmup is 128 records; there are four partitions, eight active jobs, 32-record bulk batches, and a 20 ms commit cadence in the harness. Payloads contain an ID, sequence, timestamp and 256-character text. Workloads: identity, 5 ms simulated I/O per message, 5 ms per bulk batch, and one transient failure in every ten records. Real Kafka output and source commits are observed independently. The generator's output consumer is concurrent with input publication.

Engines:

- `original`: original source with early gevent patching and stock single-message fire-and-forget output.
- `original_ack`: benchmark-only wrapper forces single-message output ACK waiting; source files remain unchanged. This does **not** repair the original coordinator, DLQ or aggregation bugs.
- `async`: new runtime with native async functions.
- `sync`: same new runtime with sync functions in bounded executors.

Bulk output in the original already waits for ACKs under the harness setting. “ACK variant” is a narrower comparison of output completion, not full delivery equivalence. The new runtime enforces a stricter bound on all uncommitted fetched records; original pending bounds do not enforce the same invariant.

Results contain throughput to final output, throughput to observed source commits, p50/p95/p99 end-to-end latency, missing/duplicate/DLQ counts, worker CPU seconds and sampled peak RSS. Timeout trials remain explicit errors and are excluded from successful-trial medians; always report success counts. The driver stops a timed-out trial without claiming a cause from the exception alone. A bounded extra-output read detects immediate duplicates, not duplicates that might appear arbitrarily later.

`QF_BENCH_PROFILE=1` enables cProfile in the new worker host and writes ignored `.prof` files. Profiling adds overhead and its throughput must not be compared directly with unprofiled runs. `bulk_diagnostic.json` records such a diagnostic run. `pipelines_before_tuning.json` and `pipelines_scale.json` retain results from the initial 500 ms aiokafka fetch wait. `pipelines_bulk_tuned.json` uses the final configurable 50 ms default; see the report for the larger-batch tradeoff.

## HTTP load

```bash
python benchmarks/compare_http.py --requests 1000 --repeats 3
```

Real loopback TCP, dynamic JSON routes, schema validation, one server process, concurrency 1/16/64/256, 128-request warmup, three repetitions, alternating engine order, identity and 5 ms simulated I/O. Original gevent WSGI has TCP_NODELAY enabled to avoid a transport artifact; FastAPI uses Uvicorn's asyncio loop. This bounded HTTPX driver measures the combined client/server path, including client connection-pool contention; it is not an independent saturation-capacity certification. Neither run measures authentication, large payloads, database access or TLS.

## Reliability probes and tracing

```bash
python benchmarks/original_probes.py
```

The original probes load selected original functions via AST, inject failures at transport boundaries, and execute the original aggregation Lua against real Redis. They are executable defect reproductions, separate from process-level crash tests of the new framework. [Tracing.md](../docs/Tracing.md) describes the four-process real Jaeger proof.

Raw JSON and dependency snapshots are tracked under [results](results). Detailed methodology, complete tables, failure evidence and deployment limits are in the [comparison report](../docs/qf_flask_vs_qf_fastapi_report.md). No throughput thresholds are asserted in CI: shared runners have unstable performance. CI tests behavioral bounds and correctness; performance regressions require comparison on a controlled host.
