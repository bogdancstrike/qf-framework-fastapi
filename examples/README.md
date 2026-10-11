# Examples

Run commands from the repository root after `pip install -e '.[dev,tracing]'`.

| Example | Start here | Requires | Demonstrates |
|---|---|---|---|
| HTTP and legacy configuration | `uvicorn examples.app:app --port 4000` | Python | Uppercase Config, endpoint JSON, typed paths, nullable models, legacy request adapter |
| Single, bulk and join workers | [workers.py](workers.py) | Kafka; Redis for join | Existing decorators with native async handlers; enable using `QSINT_ENABLE_ETL=true` |
| Complete text pipeline | [pipeline/README.md](pipeline/README.md) | Kafka + Redis | HTTP ingress, acknowledged publication, fanout, two workers, correlation join, output verification, optional Jaeger |

The first example starts without external services by default. The complete pipeline deliberately enables workers and uses its own topic namespace. Use one example per process: decorators register workers process-wide, with one owner per input topic.

For recipes beyond these executable applications, see the [worker guide](../docs/Workers.md). For measured throughput, use the [benchmark harnesses](../benchmarks/README.md), which include warmup, repetitions and committed-offset checks; the demonstration client is a functional walkthrough.
