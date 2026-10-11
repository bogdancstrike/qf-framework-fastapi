# Testing and validation

Correct output, committed progress, recovery and speed are separate checks. This guide explains which command verifies each. Install `pip install -e '.[dev,tracing]'` and run from the repository root.

The current full local profile passes **189 tests**: 170 unit/regression tests and 19 opt-in live tests. It includes six new independent-process replica scenarios. Historical benchmark/test checkpoints remain recorded separately in [validation evidence](../benchmarks/results/validation.json).

```bash
QF_INTEGRATION=1 python -m pytest tests/test_horizontal_recovery.py -v
```

This checks scale-up, active SIGKILL/replacement, committed partial joins after total worker restart and excess idle consumers beyond partition count. [Scaling](Scaling.md) explains the persistence/retention assumptions; [Hardening](Hardening.md) reports the separate 10k source-ledger and join performance measurements.

## Fast local checks

```bash
python -m pytest -q
ruff check .
ruff format --check .
python -m build
```

The default pytest run exercises unit/regression tests and skips service-dependent tests. It does not prove your Kafka or OTLP deployment works. CI runs the unit suite on Python 3.11, 3.12 and 3.13, plus a dedicated live-service job. Throughput thresholds are intentionally absent from shared CI runners.

## Real-service profiles

```bash
docker compose -p qf-fastapi-test up -d --wait

# Kafka/Redis workers, commits, retry/DLQ, stress and process crash/restart.
QF_INTEGRATION=1 python -m pytest tests/test_e2e.py tests/test_crash_recovery.py -v

# Run the documented application and CLI, checking the final joined output.
QF_INTEGRATION=1 python -m pytest tests/test_example_pipeline.py -v

# Four independent processes, real OTLP export, Jaeger storage verification.
QF_JAEGER=1 python -m pytest tests/test_otlp_jaeger.py -v

# More records through bounded capacity and transient retries.
QF_INTEGRATION=1 QF_STRESS_MESSAGES=10000 python -m pytest tests/test_e2e.py -k stress -v

# Dedicated broker pause, failed readiness, restart and replay.
QF_INTEGRATION=1 QF_CHAOS=1 python -m pytest tests/test_chaos.py -v
```

Run chaos separately from other load. It pauses only `qf-fastapi-test-kafka-1`; it still interrupts every application using that broker during the pause. Tests create isolated topics; use the dedicated compose project. Live tests support `QF_TEST_KAFKA` and `QF_TEST_REDIS`; tracing additionally supports `QF_TEST_OTLP` and `QF_TEST_JAEGER`. The chaos profile specifically depends on the named local container.

The complete local profile is:

```bash
QF_INTEGRATION=1 QF_CHAOS=1 QF_JAEGER=1 QF_STRESS_MESSAGES=10000 python -m pytest -q
```

## Test your own business transformation

Given the `myapp/workers.py` function in [GettingStarted.md](GettingStarted.md), save as `tests/test_orders.py` in your application:

```python
import pytest

from myapp.workers import validate_order


async def test_order_validation():
    output = await validate_order({"id": "order-1", "quantity": 2}, "test", {})
    assert output == {"id": "order-1", "quantity": 2, "validated": True}


async def test_invalid_order_is_rejected():
    with pytest.raises(ValueError, match="quantity must be positive"):
        await validate_order({"id": "order-2", "quantity": 0}, "test", {})
```

Use pytest-asyncio with `asyncio_mode = "auto"` in your application's pytest configuration, or mark async tests with `@pytest.mark.asyncio`. A direct call verifies business logic only; the worker decorators register metadata. It does not test retries, Kafka acknowledgements, backpressure or source commits.

For service integration, create a UUID-scoped input/output/DLQ topic set, start the actual runtime or serving process, publish known IDs, collect and validate outputs, and query source commits through the broker. Use a unique group and correlation namespace. Stop all clients/processes and delete only the test's topics in `finally` cleanup. [test_e2e.py](../tests/test_e2e.py) and [test_example_pipeline.py](../tests/test_example_pipeline.py) are concrete starting points.

## Assertions that catch delivery defects

| Scenario | Assert |
|---|---|
| Normal processing | Every expected ID and application field appears; source commits advance through inputs |
| Out-of-order completion | The coordinator does not commit across unfinished fetched work |
| Failed commit | Completed candidates remain retryable until broker acknowledgement |
| Handler retry | Attempt count is bounded; every invocation sees a fresh input copy |
| Bad input / exhausted attempts | Expected diagnostic DLQ count and source coordinates; source completes only after DLQ ACK |
| Output/DLQ transport failure | Readiness fails on exhaustion and unfinished source remains replayable |
| Process killed after output | Restart demonstrates the expected replay/duplication boundary with and without ledger |
| Redis join | Branch fields merge deterministically; duplicate arrivals/lease renewal do not lose retained state |
| Capacity saturation | Active jobs and **all** fetched uncommitted records stay within configured bounds |
| Tracing | Final Kafka trace ID matches HTTP; all expected services/spans exist and parent references resolve |

Distinguish a bounded duplicate observation window from a proof that duplicates can never appear. Account for TTL, consumer-group changes, output-ACK-to-ledger windows and Redis durability when defining expected behavior.

## Reproduce performance measurements

Set up the original's separate environment before using both-engine comparisons. Both runtimes import `framework`, so installing them together invalidates the experiment. Use different output paths for reruns to preserve published evidence:

```bash
python benchmarks/compare_pipelines.py --messages 1000 --repeats 3 \
  --engines original_ack,async --workloads noop,io,bulk,retry \
  --output benchmarks/results/my_1k_run.json

python benchmarks/compare_pipelines.py --messages 100000 --repeats 3 --concurrency 8 \
  --engines original_ack,async --workloads noop,io,bulk,retry \
  --timeout 900 --stall-timeout 60 --progress-interval 30 \
  --output benchmarks/results/my_100k_run.json
```

The second command can take substantial time; its timeout is per phase/trial, not the whole command. For HTTP, see the [benchmark command reference](../benchmarks/README.md#http-load). Worker benchmarks measure one stage, not the entire fanout/join graph. A functional demo with three jobs is not a load test.

Compare the same output acknowledgement mode, payload, partition count, worker concurrency, batch size, warmup and commit cadence. Report elapsed output time and source-commit time, throughput, p95/p99 latency, errors, missing/duplicate counts, success/attempt counts, CPU/RSS and host details. Preserve failed trials. The original ACK harness changes output waiting but does not repair all original delivery defects; it is not full semantic equivalence.

The published [comparison](qf_flask_vs_qf_fastapi_report.md) contains both 1,000 and 100,000-message results. Re-run controlled trials after meaningful runtime/dependency changes; avoid comparing results collected during profiling, chaos tests or competing host load. Production acceptance still needs representative external services, replicated Kafka, realistic Redis durability and a sustained soak outside this local test stack.
