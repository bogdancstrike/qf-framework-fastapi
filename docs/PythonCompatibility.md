# Python compatibility

Python **3.15.0 stable** was released on October 9, 2026; this framework was checked against the final release, not an alpha/RC. See the [official release page](https://www.python.org/downloads/release/python-3150/).

## Executed checks

| Interpreter | Unit/regression and package checks | Live worker validation |
|---|---|---|
| CPython 3.11 | GitHub CI | Not separately run on this version |
| CPython 3.12 | GitHub CI; local 197 unit tests | Local full 221-test profile and CI service job |
| CPython 3.13 | GitHub CI | Not separately run on this version |
| CPython 3.14 | GitHub CI | Not separately run on this version |
| CPython 3.15.0 | Local 197 unit tests; CI unit/build job; clean wheel HTTP smoke | Local full **221 tests passed in 186.47 s**; dedicated CI service job |

All seven jobs passed for the final audited runtime in [CI run 38171953565](https://github.com/bogdancstrike/qf-framework-fastapi/actions/runs/38171953565): five interpreter unit/build jobs and two live-service jobs. The initial 189-test compatibility checkpoint remains archived separately; the current audit has 221 tests.

The default profile skips 24 opt-in live tests. On 3.15 the complete local profile has zero skips/failures, covering real Kafka/Redis, single/sync/bulk/join handlers, retries/DLQ, 10,000-message stress, active SIGKILL and replacement, scale-up, committed partial joins after total worker restart, six replicas sharing four partitions, broker outage and four-service OTLP-to-Jaeger propagation. The existing Starlette TestClient/httpx deprecation warning remains; it did not cause a failure.

No framework runtime changes, forced pure-Python transport fallback or unsupported ABI compatibility flags were needed for 3.15. The declared dependency ranges installed successfully and `pip check` passed. Some new-interpreter native packages built from source, so fresh installations may need a C build toolchain until wheels are available for the chosen platform.

## Dependency and build evidence

Key tested versions on 3.15:

| Component | Version |
|---|---|
| FastAPI / Starlette | 0.143.0 / 1.7.0 |
| Pydantic / pydantic-core | 2.14.1 / 2.50.1 |
| aiokafka | 0.13.0 |
| Redis client | 6.4.0 |
| Uvicorn / uvloop | 0.54.0 / 0.23.0 |
| OpenTelemetry SDK / gRPC | 1.45.1 / 1.84.0 |
| pytest / pytest-asyncio | 9.1.1 / 1.4.0 |

The local interpreter was built in `/tmp/qf-python315` from the official source archive, whose SHA-256 was verified against Python.org. The system Python and historical benchmark environment were not replaced. It is a standard GIL build on Linux x86_64, without PGO/LTO; unrelated optional standard-library extensions were not all built. The hosted CI uses the setup-python distribution independently.

The wheel/sdist build passed on 3.15. A separate clean environment installed the wheel with runtime dependencies only, without tracing SDK extras, and passed actual HTTP readiness, liveness, OpenAPI and Prometheus requests. The runnable `SingleExecute` example produced one execution and four skips.

Exact interpreter/configuration, installed versions and results are in [python315_validation.json](../benchmarks/results/python315_validation.json), [dependency freeze](../benchmarks/results/python315_dependencies.txt) and [3.15 replica evidence](../benchmarks/results/horizontal_recovery_python315.json). The [workflow](../.github/workflows/ci.yml) tests Python 3.11–3.15 and runs live-service jobs on both 3.12 and 3.15.

## Reproduce

With a final Python 3.15 interpreter installed, run from the repository root:

```bash
python3.15 -m venv /tmp/qf-fastapi-py315
source /tmp/qf-fastapi-py315/bin/activate
python -m pip install -e '.[dev,tracing]'
python -m pip check
python -m pytest -q
docker compose -p qf-fastapi-test up -d --wait
QF_INTEGRATION=1 QF_CHAOS=1 QF_JAEGER=1 QF_STRESS_MESSAGES=10000 \
  QF_RECOVERY_RESULTS=/tmp/qf-py315-recovery.json python -m pytest -q
python -m build --outdir /tmp/qf-py315-dist
```

Do not run the broker-pause test while another workload uses the same dedicated Kafka container. Fresh package installs may resolve newer versions within the declared ranges; retain the dependency freeze for each acceptance run. A successful latest-version run does not validate every historical lower-bound combination.

## Scope of support

These checks establish the exercised Linux CPython behavior. Free-threaded `3.15t`, optional JIT configurations, PyPy, Windows/macOS deployment and multi-node infrastructure failover were not validated here.

The 1k/100k Flask comparisons and later 10k hardening benchmarks were measured on Python 3.12.3. They remain unchanged and do **not** establish a Python 3.15 speedup. The full-suite elapsed time includes process synchronization, group timeouts and test infrastructure; comparing full-suite durations is not a throughput benchmark.
