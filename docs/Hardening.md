# ACK, join and replay-ledger hardening

Changes preserve the existing worker/config/tracing conventions and at-least-once delivery. The source ledger remains optional; joins continue using Redis correlation state. Historical 1,000/100,000-message Flask comparisons remain intact in the [comparison report](qf_flask_vs_qf_fastapi_report.md). These follow-up numbers compare the previous and hardened **FastAPI** runtime, not Flask.

## Correctness changes

- `@kafka_handler(aggregator=True)` selects a join with default `id`; `aggregate_by="order.id"` selects a custom nested key. The compatibility aggregator decorator remains. Missing/invalid correlation values go to DLQ; bulk aggregation is rejected.
- Each output/DLQ retains its delivery receipt across transport attempts. Known ACKs are skipped and in-flight sends survive ACK-wait timeouts. Confirmed send failures can be retried; ambiguous failures and process loss still permit duplicates.
- Redis claims reuse one token across retries. Token-valued completion receipts make a lost completion reply retryable without incorrectly declaring ownership lost. Old tombstones remain recognized.
- Source-ledger claims, finish, release and renew operations are pipelined, using cached Lua scripts and one heartbeat per job. Each script is atomic; the batch is not a cross-key transaction. Mixed replay batches count skipped records correctly.
- New metrics cover output ACK count/latency, ledger claimed/skipped/wait outcomes and join stored/ready/busy/done arrivals.
- The utility audit fixes fast SingleExecute lease loss, stale circuit completion, resource startup/cleanup, finite JSON and byte/APDU boundary errors. See [Utilities](Utilities.md).
- Terminal worker errors now fail HTTP liveness as well as readiness, enabling configured Kubernetes restarts. Multi-process scaling/crash checks are documented in [Scaling](Scaling.md).

## Measured 10,000-record source-ledger workloads

Real Kafka/Redis; four partitions, eight worker jobs, 32-record bulk batches, 5 ms simulated I/O per bulk job, 128-record warmup, three repetitions per workload/version, 20 ms commit cadence. Replay ledger enabled in both versions. Medians end at final observed output; source commits are verified separately.

| Workload | Before time | After time | Before msg/s | After msg/s | Throughput ratio | Worker CPU before → after |
|---|---:|---:|---:|---:|---:|---:|
| Identity | 7.248 s | 6.957 s | 1,380 | 1,437 | **1.042×** | 2.77 → 3.23 s |
| 32-record bulk | 1.632 s | 0.796 s | 6,126 | 12,560 | **2.050×** | 1.51 → 0.59 s |

Bulk elapsed time fell about 51.2%; worker CPU fell about 60.9%. Identity throughput increased about 4.2%, while CPU increased about 16.6% and median sampled RSS rose from 56.0 to 60.5 MiB. Receipt callbacks, metrics and stronger bookkeeping have costs. Three trials on a shared host do not establish statistical significance for the small identity difference.

| Workload | Commit-complete msg/s, before → after | End-to-end p95, before → after | Median peak RSS, before → after |
|---|---:|---:|---:|
| Identity | 1,377 → 1,431 | 6,615.7 → 6,345.4 ms | 56.0 → 60.5 MiB |
| Bulk | 6,085 → 12,391 | 1,213.8 → 431.5 ms | 61.7 → 57.9 MiB |

All 12 trials completed: 120,000 measured input-record instances, zero observed missing outputs, duplicates or DLQ. The successful-run duplicate observation window is bounded; failure tests separately expose replay duplicates.

## Two-branch join measurement

Each trial joins 10,000 correlations from 20,000 accepted source records, with four partitions per topic and eight jobs. There are three trials per version, a 128-correlation warmup and a custom `order.id` key. The driver and runtime share one process; its resource accounting is not comparable to the separate-worker pipeline harness.

| Metric | Previous FastAPI | Hardened FastAPI |
|---|---:|---:|
| Median final-output time | 7.134 s | 7.141 s |
| Median joined outputs/s | 1,402 | 1,400 |
| Median commit-complete joins/s | 1,397 | 1,394 |
| End-to-end p95 | 6,705.0 ms | 6,711.2 ms |

Observed throughput ratio is **0.999×**, effectively unchanged. All six trials completed with zero missing, duplicate or DLQ outputs: 60,000 joined outputs from 120,000 measured source-record instances. Stronger retry/ownership behavior did not produce a measurable speed gain in this workload.

## Reproduction and limits

Baseline runtime: `64350bc`; source-ledger after: `0a41944`; join after: `7158ed1`. Raw artifacts: [ledger before](../benchmarks/results/reliability_before.json), [ledger after](../benchmarks/results/reliability_after.json), [join before](../benchmarks/results/joins_before.json), [join after](../benchmarks/results/joins_after.json), [revision/hash provenance and summary](../benchmarks/results/hardening_provenance.json). Commands are in the [benchmark guide](../benchmarks/README.md).

Trials ran before-then-after rather than alternating. The same local single-broker/Redis stack and shared host were used. RF1 does not validate broker failover durability. No previous 100k result is presented as a fresh measurement of this revision. These reliability changes narrow duplicate windows but do not create an atomic Kafka/Redis/business transaction.

The regression suite exercises partial fanout ACK failure, late ACK timeout, partial bulk DLQ failure, lost Redis claim/completion replies, concurrent replay claims, invalid aggregation configuration and the utility failure paths. Real process tests additionally scale 2 → 4 replicas, kill/replace active workers, preserve committed partial joins across full worker restart and verify six replicas sharing four partitions. See the [scaling guide](Scaling.md) for recovery evidence.

Primary references: [aiokafka producer semantics](https://aiokafka.readthedocs.io/en/stable/producer.html), [Redis Lua atomicity](https://redis.io/docs/latest/develop/programmability/eval-intro/), [asyncio cancellation](https://docs.python.org/3.13/library/asyncio-task.html).
