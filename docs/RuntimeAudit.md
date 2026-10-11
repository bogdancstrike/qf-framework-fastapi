# Runtime correctness and extension audit

This follow-up preserves worker decorators, configuration files and tracing
helpers. It strengthens concurrent state transitions and startup validation,
adding [extension contracts](Extending.md) rather than a new plugin system.

## Reproduced defects and fixes

| Trigger | Consequence before | Corrected behavior |
|---|---|---|
| Old job succeeds after another opens the worker circuit | New circuit closes | Admission generations fence stale completions |
| Half-open job cancelled, including before its first task step | Probe remains occupied | Cancellation releases the matching probe generation |
| Redis processing lease lost while Kafka partition remains assigned | Failed job leaves a permanent uncommitted gap with healthy readiness | Terminal failure becomes visible; supervision restarts and replays uncommitted input |
| Join timeout shorter than the heartbeat interval | Complete parts expire under a valid lease | Complete parts retain lease duration plus join timeout; renewals refresh both |
| Joined handler exhausts application retries | Acknowledged DLQ reported as circuit success | Join propagates failed-handler outcome after fenced completion |
| Injected workers share a name/input topic | Runtime dictionaries silently overwrite workers | Pure validation rejects collisions before connection |
| Caller mutates topics/nested metadata after startup | Active configuration can change | Startup snapshots configuration containers |
| Environment and dotenv use different aliases | File value overrides process value | Resolve aliases within each precedence layer |
| Infinite timeout values | Bounded operations become unbounded | Settings reject nonfinite floats |
| Falsy malformed models or overlapping parameterized paths | Invalid mapping or unreachable route | Reject invalid containers, equivalent route patterns and duplicate operation IDs |
| Cleanup/factory fails during lifecycle transitions | Tracing flush/signal cleanup skipped; primary error hidden | Independent cleanup runs; primary error survives secondary cleanup failures |

There are 27 focused cases in [test_runtime_audit.py](../tests/test_runtime_audit.py).
The live lease test now uses a one-second join timeout and three-second lease,
recovering complete parts across multiple heartbeat intervals.

## Repeated 5,000-message, five-worker pipeline

Five independent trials run per interpreter, each with four partitions, five
sequential workers, six Kafka boundary topics and a DLQ. Every trial injects
recoverable handler failures at every stage. Affected jobs fail twice and succeed
on their third attempt. Selected third-stage sends fail before enqueue; actual
producer sends and acknowledgements use real Kafka.

| Trial | Source ledger | Third-stage bulk | Graceful runtime replacement |
|---|---|---|---|
| 1 | Off | No | No |
| 2 | On | No | No |
| 3 | Off | No | No |
| 4 | On | Up to 32 records/job | No |
| 5 | On | No | After at least 500 final-stage jobs |

Assertions check every ID at every worker, correct stage order, exactly 5,000
records on every pipeline topic, no unexpected final duplicates or DLQ, exact
injected/observed retry counts and all 20 source partitions committed. Evidence:
[Python 3.12](../benchmarks/results/pipeline_reliability.json),
[Python 3.15](../benchmarks/results/pipeline_reliability_python315.json).

Five workers share one runtime process. Independent-process scale-up, SIGKILL,
full-worker replacement and excess replicas are validated separately in
[Scaling](Scaling.md). This trial's replacement is graceful. Local RF1 Kafka and
Redis do not validate storage-node failover or exactly-once business effects.

All **10/10 trials passed** across Python 3.12/3.15: 50,000 accepted inputs,
250,000 successful stage executions, 50,000 final outputs, zero missing, duplicate
or unexpected DLQ records. Every trial verified all 20 source partitions.
The runs recovered **12,174 injected handler failures** and **520
pre-enqueue transport failures** within their retry budgets.

| Trial | Python 3.12 final-output time | Python 3.15 final-output time | Final outputs per interpreter |
|---|---:|---:|---:|
| 1 | 4.327 s | 4.062 s | 5,000 / 5,000 |
| 2 | 8.480 s | 7.313 s | 5,000 / 5,000 |
| 3 | 4.293 s | 4.045 s | 5,000 / 5,000 |
| 4 | 7.651 s | 6.826 s | 5,000 / 5,000 |
| 5 | 8.500 s | 7.432 s | 5,000 / 5,000 |

Times include startup and stop at the final observed output. Commit verification
follows separately. Configurations differ across trials; these figures are
reliability timings, not a controlled Python-version throughput comparison.

## Profile-guided ACK optimization

cProfile identified repeated Prometheus `labels()` lookups in receipt callbacks.
For a 100-record output batch, callbacks previously resolved identical metric
children 200 times. The publisher now resolves them twice per job and reuses
them. ACK/retry/timeout semantics remain the same.

The microbenchmark processes 100,000 immediate in-memory receipts per trial,
batches of 100, five repetitions per version and 5,000-record warmup. It excludes
Kafka/Redis, handlers, commits and startup. It measures ACK bookkeeping rather
than end-to-end pipeline speed. A single-output job does not eliminate lookups.

Isolated ACK bookkeeping (100,000 receipts, batches of 100, five trials/version) reduced median CPU from **0.351 s to 0.248 s** (**29.5% less CPU**, 1.418× for this microbenchmark). It excludes Kafka/Redis, handlers and commits; it does not establish an end-to-end pipeline speedup.

Per-trial CPU/elapsed time, interpreter and source hashes are archived in
[before](../benchmarks/results/ack_bookkeeping_before.json) and
[after](../benchmarks/results/ack_bookkeeping_after.json). Reproduce with
[profile_ack_publisher.py](../benchmarks/profile_ack_publisher.py), selecting the
source via PYTHONPATH. Profile separately: cProfile itself changes timings.

## Validation and limits

The full final runtime suite passed **221 tests** with zero skips on both Python
3.12 (184.22 s) and 3.15 (186.47 s). The default
profile has 197 unit/regression tests and 24 opt-in service tests. Package builds
and dependency checks passed; the Starlette/httpx deprecation warning remains.

Final runtime checkpoint `8f0e055` passed all seven jobs in
[CI run 38171953565](https://github.com/bogdancstrike/qf-framework-fastapi/actions/runs/38171953565):
Python 3.11–3.15 unit/build and live-service jobs on 3.12/3.15.
Local checks include full Kafka/Redis E2E, stress, broker
outage, process recovery and actual Jaeger storage queries. Final counts and
commands are retained in [validation.json](../benchmarks/results/validation.json).

Historical Flask/FastAPI 1k/100k and earlier 10k reliability benchmarks retain
their measured revisions. This audit does not rerun those throughput comparisons.
At-least-once delivery, finite Redis retention, external business effects and
multi-node durability limits still apply.
