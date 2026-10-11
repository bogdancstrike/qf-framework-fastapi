# QF Flask/gevent versus QF FastAPI/asyncio

Measurement date: 2026-10-11. Original source revision: `d54b3acae4b902e8715455bc9464ae11565ba739`. Successor: [qf-framework-fastapi](https://github.com/bogdancstrike/qf-framework-fastapi), distribution `qf-fastapi` 2.0.0. Final runtime benchmark code corresponds to checkpoint `4c5443e`; later endpoint-model, zero-ID validation and documentation changes do not alter the worker benchmark path. The original source was not modified.

## Management summary

**Recommendation:** adopt the async framework for new pipeline work and migrate existing applications in stages. The strongest benefit is clearer failure recovery and observable operation. Performance improves in several tested workloads, but not in every workload or delivery mode.

### Throughput: what the small test actually shows

Throughput means completed output messages per second. “4.64×” means the async implementation delivered 4.64 times as many messages per second as the named comparator in this test. It does not mean every production pipeline runs 4.64 times faster. These comparisons use the original ACK-gated benchmark variant, which waits for output acknowledgements.

| Workload, 1,000 messages | Original ACK msg/s | FastAPI async msg/s | Async / original | Practical interpretation |
|---|---:|---:|---:|---|
| Identity transformation | 301 | 1,397 | **4.64×** | More acknowledged throughput in this local test |
| 5 ms simulated I/O per message | 298 | 1,331 | **4.47×** | More acknowledged throughput in this local test |
| 32-record bulk jobs | 1,712 | 15,990 | **9.34×** | More acknowledged throughput in this local test |
| 10% transient handler failures | 319 | 1,388 | **4.35×** | More acknowledged throughput in this local test |

Each value is a three-run median with four Kafka partitions and eight worker jobs. All runs in this table completed. The stock original identity path reached 11,020 msg/s by returning before output ACK; that is a different reliability tradeoff. In the 5,000-record bulk follow-up, successful original ACK trials reached 18,202 msg/s versus async 14,136 msg/s: async was **22% slower**, and one of three original trials timed out. HTTP results are mixed; there is no demonstrated large HTTP speedup.

### Resilience: what has been demonstrated

| Question management may ask | Evidence | Business implication |
|---|---|---|
| Can a failed commit silently lose progress bookkeeping? | Six original failure defects were reproduced; successor regression tests cover fixes, including retaining failed commits | Recovery behavior is explicit and reviewable |
| Can a stopped worker recover unfinished work? | New runtime passed broker pause/restart and process-kill tests | Uncommitted input is replayable; deployment supervision must restart failed runtimes |
| Can replay duplicate business actions? | Killing after 20 output ACKs produced 40 outputs without the ledger; 20 with the completed ledger | Idempotent business effects remain necessary; the optional ledger narrows, but does not eliminate, duplicates |
| Does overload have a resource boundary? | 10,000-record stress test with 1,000 injected transient failures maintained configured work/progress bounds | Backlog stays in Kafka instead of an unbounded in-process completion set |
| Can operators follow an item through the pipeline? | Real Jaeger query verified 13 linked spans in four services with one trace ID | HTTP-to-worker fanout/join investigation has an executable observability proof |
| Is this production resilience certification? | Single broker/Redis tests; no multi-node failover, Redis data loss or long soak | Deployment-specific acceptance tests remain required |

### Suggested migration gates

1. Replay representative input into isolated topics and outputs in a separate environment; verify business results and stable IDs before enabling real side effects.
2. Set throughput and tail-latency targets using production payloads and downstream services. Validate the deployment's broker/Redis failover and data-loss boundary.
3. Prepare DLQ consumers for diagnostic envelopes and make downstream business effects idempotent. Stop/drain the old deployment before a controlled consumer-group cutover; verify committed offsets and rollback behavior.
4. Monitor readiness, lag, retry/DLQ rates and traces during rollout. Treat the remaining production acceptance tests as release gates, not as covered by the local benchmark.

### Larger validation: 100,000 messages

The matched 100,000-message comparison is running with the same four workloads, four partitions and eight-worker concurrency, using `original_ack` and `async`, three repetitions each. Results will appear in `pipelines_100k.json` and a dedicated section below. **The 1,000-message ratios are not yet claims about 100,000 messages.** The driver now records whether incomplete runs stalled on output or source commits; deadlines and failed-trial counts will be reported explicitly.

## Findings and decision

The successor preserves the worker/configuration/tracing conventions while making Kafka, Redis, handler backoff and application lifecycle asynchronous. It also changes delivery semantics: input completion now waits for every output or DLQ acknowledgement, and failed commit progress remains retryable. These correctness improvements matter more than a framework-name comparison.

For the final 1,000-message, eight-job workload, native async throughput was approximately **4.64×** the original ACK-gated harness for identity, **4.47×** for 5 ms simulated I/O, **9.34×** for bulk, and **4.35×** for transient retries. These are ratios of three-trial medians on this host, not universal speedups. Stock original identity/retry paths were faster because single-message output completion returns before acknowledgement. Successful original large-bulk trials also exceeded the new runtime's throughput. HTTP differences were modest and mixed; increasing this driver's concurrency did not establish server capacity.

The migration is justified for reviewable recovery boundaries, native async integrations, bounded memory exposure, explicit lifecycle and test coverage. It is not justified by a claim that FastAPI is always faster, or that producer idempotence delivers exactly-once business effects. Use the new framework for new async pipeline development, and migrate existing workers with application-specific correctness and deployment load checks.

## Environment and experimental method

| Dimension | Configuration |
|---|---|
| Host | AMD Ryzen 9 9950X3D, 16 physical / 32 logical CPUs, Linux x86_64, all 32 CPUs available; no affinity pinning |
| Python | 3.12.3 locally; CI additionally checks 3.11 and 3.13 |
| Infrastructure | Docker Apache Kafka 3.9.1 single broker/controller; Redis 7.4 Alpine; Jaeger all-in-one 1.62.0 |
| Durability | Kafka replication factor 1; producer `acks=all`; Redis AOF everysec; no replicated storage proof |
| Worker trials | Separate worker processes and Python environments, four partitions, 128-record warmup, three repetitions, alternating engine order |
| Worker topology | One measured stage: Kafka input → worker → Kafka output; multi-stage fanout/join is separately tested for correctness/tracing, not benchmarked at 100,000 messages |
| Main workload | 1,000 records/trial, eight active jobs, bulk size 32 / age 10 ms, 20 ms commit tick |
| Larger workload | 5,000 records/trial, 32 active jobs, same four partitions and payload shape |
| Bounds | Harness pending setting 128 per partition; new runtime bounds all uncommitted records, original does not implement an equivalent bound |
| Input | JSON ID, sequence number, monotonic timestamp, 256-character text; generator explicitly chooses input partitions |
| Work | Identity; 5 ms sleep/message; 5 ms sleep/bulk batch; identity with every tenth record failing once |
| Producer | New: aiokafka idempotent producer, linger 5 ms, batch size 65,536; original: existing kafka-python producer, harness `acks=all` |
| HTTP | Real loopback TCP, one process, dynamic route plus schema, identity/5 ms sleep, 128 warmup requests, 1,000 measured requests/trial, concurrency 1/16/64/256, three repetitions |
| Excluded | TLS overhead, real database/downstream API, multi-broker failover, broker power loss, Redis failover/data loss, large messages, multi-hour soak and CPU-heavy handlers |

The worker load is a finite input burst with concurrent output collection. Latency runs from each payload timestamp immediately before producer enqueue to observation by the benchmark consumer, including input publishing, broker batching, framework queues, handler work and output delivery. It is not isolated handler service time. Commit throughput separately waits until the source group's committed offsets reach all published inputs. For original retries, those targets are the external input publisher's offsets; internal retry-topic republishes add source records and are not a separate final-offset target. All expected output IDs must still be observed before commit timing can finish. Retry payload bookkeeping also differs as documented in migration notes. HTTP latency includes HTTPX connection pooling and scheduling.

CPU is user+system time of the worker/server process during measurement. RSS is sampled process memory approximately every 10 ms; it is not a true allocator peak or total host/container memory. Generator, broker, Redis, Jaeger and kernel costs are excluded. CPU timings have coarse granularity in short trials. Every table cell is a median across successful trials; independent metric medians need not describe one actual trial. Ranges show observed output throughput variability, not statistical confidence intervals.

The host was not dedicated or pinned. Suites ran sequentially; no chaos tests ran during measured benchmark trials. Docker topic creation sometimes produced transient metadata/NotLeader responses, recovered by clients. Warmup does not eliminate all metadata/batching variation. Three repetitions and short trials are suitable for detecting large local effects, not establishing a production SLO or significance for small HTTP differences.

Full installed dependencies are captured in [original requirements](../benchmarks/results/original-requirements.txt) and [FastAPI requirements](../benchmarks/results/fastapi-requirements.txt). Paths in these snapshots describe this checkout; they are provenance, not portable lockfiles. Pin deployment dependencies separately.

## What the engines mean

| Engine | Executed code | Output completion | Reliability interpretation |
|---|---|---|---|
| `original` | Unmodified original imports; early gevent monkey patch in host | Stock single-handler path returns before output ACK; original bulk configured synchronous | Fast happy path with known output/commit failure windows |
| `original_ack` | Same source, benchmark-only wrapper sets single-handler `output_sync=True` | Single and bulk output ACK waiting | Narrows one completion difference; retains original commit/DLQ/aggregation defects |
| `async` | New runtime, native coroutine handler | All output/DLQ ACKs before completion | Manual completed-prefix commits; at least once |
| `sync` | New runtime, sync handler in bounded executor | Same as async | Migration path for existing blocking workers |

The baseline is the actual worker implementation: gevent patches the process, while the source also uses executor threads. This is not a pure greenlet-versus-coroutine microbenchmark. The original advertises `KAFKA_COMMIT_STRATEGY`, but the executable worker code does not read that setting. This report does not claim stock “commit before processing” behavior. Its observed risk is marking jobs complete before asynchronous output ACK and faulty progress bookkeeping on specific error paths.

## Main pipeline results: final 50 ms fetch-wait setting

All 48 trials completed: **48,000 measured source records**, plus warmup. Each completed trial observed zero missing messages, immediate duplicates and DLQ records. This happy-path outcome does not invalidate the independently reproduced failure defects.

| Workload | Engine | Passed | Output msg/s | Commit msg/s | Output range | p50 ms | p95 ms | p99 ms | CPU s | RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| noop | original | 3/3 | 11,020.12 | 8,230.01 | 10,999.03–11,178.59 | 48.87 | 69.57 | 72.81 | 0.09 | 52.83 |
| noop | original_ack | 3/3 | 301.30 | 299.36 | 298.49–301.41 | 1,654.02 | 3,138.82 | 3,273.03 | 0.25 | 52.27 |
| noop | async | 3/3 | 1,397.27 | 1,374.06 | 1,389.59–1,399.05 | 351.09 | 662.08 | 691.34 | 0.10 | 42.81 |
| noop | sync | 3/3 | 1,396.90 | 1,375.91 | 1,384.20–1,444.50 | 351.31 | 665.13 | 687.80 | 0.13 | 43.02 |
| io | original | 3/3 | 300.20 | 298.30 | 300.19–300.41 | 1,664.12 | 3,149.02 | 3,284.85 | 0.26 | 52.25 |
| io | original_ack | 3/3 | 297.69 | 293.99 | 297.29–299.54 | 1,695.76 | 3,179.90 | 3,314.85 | 0.26 | 52.32 |
| io | async | 3/3 | 1,331.21 | 1,294.92 | 1,326.23–1,348.15 | 373.30 | 694.40 | 722.20 | 0.10 | 42.87 |
| io | sync | 3/3 | 863.23 | 855.09 | 819.43–885.95 | 557.24 | 1,083.55 | 1,129.55 | 0.19 | 43.16 |
| bulk | original | 3/3 | 1,696.40 | 1,636.57 | 1,696.18–1,698.53 | 45.29 | 565.79 | 570.66 | 0.06 | 52.62 |
| bulk | original_ack | 3/3 | 1,712.00 | 1,651.10 | 1,696.65–1,742.48 | 46.61 | 562.05 | 566.85 | 0.06 | 52.69 |
| bulk | async | 3/3 | 15,990.23 | 14,313.64 | 14,192.38–16,995.33 | 23.91 | 39.28 | 44.41 | 0.02 | 43.01 |
| bulk | sync | 3/3 | 15,121.97 | 14,812.32 | 14,884.46–16,964.99 | 20.00 | 43.34 | 48.35 | 0.02 | 43.34 |
| retry | original | 3/3 | 6,351.24 | 5,660.97 | 6,342.25–6,449.45 | 66.97 | 126.13 | 144.63 | 0.11 | 52.68 |
| retry | original_ack | 3/3 | 319.20 | 319.15 | 313.13–354.18 | 1,695.92 | 2,912.19 | 3,085.78 | 0.28 | 52.26 |
| retry | async | 3/3 | 1,388.38 | 1,374.37 | 1,379.72–1,395.35 | 349.67 | 666.40 | 694.81 | 0.10 | 42.94 |
| retry | sync | 3/3 | 1,416.04 | 1,354.76 | 1,402.15–1,447.05 | 348.71 | 649.66 | 677.28 | 0.15 | 43.11 |

[Raw final worker trials](../benchmarks/results/pipelines.json).

Native async identity throughput is about 7.89× lower than stock original identity, and retry throughput about 4.57× lower than stock original retry. Waiting for acknowledgements and enforcing progress bounds has a cost. The benchmark-only ACK wrapper makes the original closer on one output boundary, but cannot make the two frameworks identical in producer, retry, scheduler and commit implementation.

The new sync path is close to native async for trivial work and bulk on this host, while native async handles the per-record simulated wait more efficiently. Async syntax alone does not accelerate CPU-bound Python. Thread-offloaded workers remain subject to the GIL, thread scheduling and cancellation limitations.

<!-- SCALE_COMPARISON_START -->
## Processing 1,000 versus 100,000 messages

In progress: 21 of 24 planned trials recorded. All 100,000-message numbers are preliminary until three repetitions per engine/workload finish.

Same four partitions, eight worker jobs and three repetitions per engine/workload. The comparator is the **original ACK-gated harness variant**; it changes single-output waiting without repairing the original's other reliability defects. Output time measures the complete input burst to final observed output. Throughput is messages/second; higher is better. Time is seconds; lower is better.

| Workload | Messages | Flask ACK output s | Async output s | Flask ACK msg/s | Async msg/s | Async / Flask rate | Flask passed/attempted | Async passed/attempted |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Identity | 1,000 | 3.32 | 0.72 | 301.30 | 1,397.27 | 4.64× | 3/3 | 3/3 |
| Identity | 100,000 | 144.23 | 72.34 | 693.33 | 1,382.42 | 1.99× | 3/3 | 3/3 |
| 5 ms I/O / message | 1,000 | 3.36 | 0.75 | 297.69 | 1,331.21 | 4.47× | 3/3 | 3/3 |
| 5 ms I/O / message | 100,000 | 376.17 | 74.92 | 265.84 | 1,334.74 | 5.02× | 3/3 | 3/3 |
| 32-record bulk | 1,000 | 0.58 | 0.06 | 1,712.00 | 15,990.23 | 9.34× | 3/3 | 3/3 |
| 32-record bulk | 100,000 | 49.70 | 5.99 | 2,012.00 | 16,707.12 | 8.30× | 3/3 | 3/3 |
| 10% transient retry | 1,000 | 3.13 | 0.72 | 319.20 | 1,388.38 | 4.35× | 3/3 | 3/3 |
| 10% transient retry | 100,000 | 290.18 | 71.57 | 344.61 | 1,397.14 | 4.05× | 1/1 | 2/2 |

Each cell is a median across successful trials. `0/0` means not yet attempted; failed attempts remain in the success counts and raw data. A ratio of 2.00× means twice the throughput, approximately half the elapsed time for the same count; a ratio below 1.00× favors Flask.

### Source-commit completion time

This separately waits until the broker reports source offsets committed through the published inputs. It is not atomic output-plus-input delivery or an exactly-once guarantee.

| Workload | Messages | Flask ACK commit s | Async commit s | Flask ACK commit msg/s | Async commit msg/s |
| --- | --- | --- | --- | --- | --- |
| Identity | 1,000 | 3.34 | 0.73 | 299.36 | 1,374.06 |
| Identity | 100,000 | 144.25 | 72.37 | 693.23 | 1,381.81 |
| 5 ms I/O / message | 1,000 | 3.40 | 0.77 | 293.99 | 1,294.92 |
| 5 ms I/O / message | 100,000 | 376.21 | 74.92 | 265.81 | 1,334.74 |
| 32-record bulk | 1,000 | 0.61 | 0.07 | 1,651.10 | 14,313.64 |
| 32-record bulk | 100,000 | 49.72 | 5.99 | 2,011.07 | 16,705.26 |
| 10% transient retry | 1,000 | 3.13 | 0.73 | 319.15 | 1,374.37 |
| 10% transient retry | 100,000 | 290.18 | 71.60 | 344.61 | 1,396.72 |

### Does the small-test advantage hold at 100,000 messages?

| Workload | 1,000-message ratio | 100,000-message ratio | Interpretation |
| --- | --- | --- | --- |
| Identity | 4.64× | 1.99× | Async remains faster; advantage is smaller at scale |
| 5 ms I/O / message | 4.47× | 5.02× | Small-test advantage retained or increased |
| 32-record bulk | 9.34× | 8.30× | Async remains faster; advantage is smaller at scale |
| 10% transient retry | 4.35× | 4.05× | Preliminary; repetitions still running |

Identity's relative gain falls because original throughput rises from 301 to 693 msg/s, while async stays near 1,397 to 1,382 msg/s. A smaller ratio therefore does not mean async throughput collapsed; the original benefits more from the larger burst.


### 100,000-message latency and process resources

| Workload | Engine | Passed/attempted | p50 ms | p95 ms | p99 ms | CPU s | RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Identity | original_ack | 3/3 | 68,160.79 | 129,493.29 | 137,688.09 | 89.95 | 63.74 |
| Identity | async | 3/3 | 34,673.04 | 65,902.93 | 68,650.39 | 15.65 | 123.23 |
| 5 ms I/O / message | original_ack | 3/3 | 195,005.14 | 354,668.16 | 369,538.09 | 56.11 | 64.61 |
| 5 ms I/O / message | async | 3/3 | 36,030.95 | 68,318.88 | 71,232.12 | 15.85 | 124.53 |
| 32-record bulk | original_ack | 3/3 | 9,457.35 | 42,217.84 | 45,347.18 | 14.77 | 66.21 |
| 32-record bulk | async | 3/3 | 1,342.49 | 2,532.41 | 2,869.48 | 3.10 | 87.27 |
| 10% transient retry | original_ack | 1/1 | 149,005.36 | 271,434.82 | 285,837.63 | 125.95 | 65.83 |
| 10% transient retry | async | 2/2 | 34,316.74 | 65,193.38 | 67,967.75 | 25.26 | 121.59 |

### Completion and resilience observations

21 completed 100,000-message trials and 0 incomplete attempts are currently recorded. Completed trials contain 0 missing outputs, 0 immediate duplicates and 0 DLQ records. This is happy-path stress evidence; it does not replace the separate crash/outage tests.

The large-run driver uses incremental unique-ID tracking to avoid repeated large set reconstruction. The payload, workers and measurement boundaries are unchanged; it also emits progress and records explicit output/source-commit stall diagnostics. Per-phase overall deadline is 900 seconds; no-progress deadline is 60 seconds. Execution order alternates across repetitions. Warmup is 128 records, excluded from measurement. Single broker, loopback transport and finite bursts remain the main experimental limits.

<!-- SCALE_COMPARISON_END -->

## Larger bursts, tuning and failures

Initial 5,000-record / 32-job measurements used aiokafka's 500 ms fetch wait. They revealed a large bulk slowdown when bounded admission repeatedly paused/seeks/refetched data. Profiling showed waiting, not JSON processing, dominated a diagnostic run: 2.395 seconds in epoll poll versus 0.317 seconds executing event-loop callbacks. Profiling itself added overhead, so that run is not included in throughput comparisons. Source review identified outstanding broker fetch waits as a plausible contributor; the measured controlled setting change supports that explanation without proving it is the only bottleneck.

### Initial larger-burst measurements (500 ms new-client fetch wait)

| Workload | Engine | Passed | Output msg/s | Commit msg/s | Output range | p50 ms | p95 ms | p99 ms | CPU s | RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| io | original_ack | 3/3 | 863.98 | 860.70 | 863.65–875.04 | 2,628.18 | 5,238.32 | 5,410.73 | 0.79 | 58.11 |
| io | async | 3/3 | 1,565.60 | 1,554.87 | 1,387.14–1,601.11 | 524.97 | 2,526.75 | 2,997.62 | 0.35 | 46.51 |
| io | sync | 3/3 | 1,482.37 | 1,468.47 | 1,464.06–1,496.27 | 768.52 | 2,799.45 | 3,186.60 | 0.66 | 49.79 |
| bulk | original_ack | 2/3 | 16,084.52 | 15,043.51 | 15,037.18–17,131.85 | 93.06 | 150.93 | 167.69 | 0.26 | 57.11 |
| bulk | async | 3/3 | 4,002.24 | 3,974.28 | 3,991.46–4,009.18 | 87.21 | 1,102.88 | 1,108.48 | 0.11 | 46.06 |
| bulk | sync | 3/3 | 4,024.04 | 3,988.44 | 2,215.19–6,446.42 | 89.05 | 1,098.62 | 1,103.69 | 0.13 | 46.39 |

[Raw initial scale trials](../benchmarks/results/pipelines_scale.json). One of three original bulk trials reached the 60-second deadline; it is retained as an error, not silently removed. Successful original bulk medians use two trials. This timeout does not by itself identify whether output collection or source commit observation stalled; the harness recorded no phase-specific timeout diagnostics. Do not present it as proven record loss or a formally estimated production failure rate.

### Repeated bulk measurements after tuning the new client to 50 ms

| Workload | Engine | Passed | Output msg/s | Commit msg/s | Output range | p50 ms | p95 ms | p99 ms | CPU s | RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| bulk | async | 3/3 | 14,135.69 | 14,119.74 | 14,134.57–14,538.49 | 83.84 | 165.43 | 176.07 | 0.10 | 45.93 |
| bulk | sync | 3/3 | 14,454.41 | 13,218.09 | 12,390.34–14,956.30 | 88.46 | 196.21 | 201.40 | 0.12 | 46.67 |
| bulk | original_ack | 2/3 | 18,202.48 | 16,287.48 | 17,631.63–18,773.32 | 79.02 | 133.11 | 141.44 | 0.24 | 57.17 |

[Raw tuned bulk trials](../benchmarks/results/pipelines_bulk_tuned.json). The original implementation and its settings were unchanged in this follow-up. One original trial again timed out; two completed. All six new-runtime trials completed with no missing/duplicate/DLQ records. Native async median improved from 4,002.24 to 14,135.69 msg/s, about **3.53×**, and p95 fell from 1,102.88 to 165.43 ms. The new default `fetch_max_wait_ms=50` is validated/configurable; a shorter wait can increase idle broker request traffic.

Successful original follow-up bulk throughput was 18,202.48 msg/s versus native async 14,135.69: the new runtime is about 22% slower on this larger bulk case. This is a meaningful remaining performance limit. The framework also enforces a stricter uncommitted-record bound, and the old coordinator is not fully repaired by the ACK wrapper. Do not trade these invariants away solely to improve the table.

The original main dataset before tuning is preserved in [pipelines_before_tuning.json](../benchmarks/results/pipelines_before_tuning.json); the profiled diagnostic is [bulk_diagnostic.json](../benchmarks/results/bulk_diagnostic.json). Initial large-I/O results are historical measurements with the old fetch wait, not final-setting capacity claims. There was no full final-setting large-I/O rerun.

## HTTP load and high-concurrency behavior

All **48,000 measured requests** across 48 trials completed with zero application/HTTP errors. TCP_NODELAY was enabled for original gevent WSGI to avoid delayed-ACK/Nagle artifacts in initial exploration. Uvicorn used the asyncio loop rather than attributing optional uvloop effects to FastAPI. The new middleware records per-route metrics; request access logging is off in both benchmark setups.

| Workload | Clients | Engine | req/s | p50 ms | p95 ms | p99 ms | Errors | CPU s | RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| noop | 1 | flask_gevent | 2,105.04 | 0.45 | 0.60 | 0.72 | 0 | 0.16 | 56.89 |
| noop | 1 | fastapi_async | 2,397.03 | 0.40 | 0.52 | 0.63 | 0 | 0.11 | 53.62 |
| noop | 16 | flask_gevent | 1,443.20 | 4.55 | 38.40 | 65.82 | 0 | 0.17 | 57.14 |
| noop | 16 | fastapi_async | 1,407.53 | 4.61 | 38.10 | 63.47 | 0 | 0.11 | 53.79 |
| noop | 64 | flask_gevent | 1,334.82 | 35.69 | 131.03 | 193.70 | 0 | 0.17 | 57.17 |
| noop | 64 | fastapi_async | 1,299.94 | 36.28 | 133.79 | 194.99 | 0 | 0.11 | 53.79 |
| noop | 256 | flask_gevent | 900.59 | 184.21 | 752.13 | 986.76 | 0 | 0.19 | 57.54 |
| noop | 256 | fastapi_async | 834.04 | 190.00 | 765.55 | 1,099.43 | 0 | 0.14 | 54.09 |
| io | 1 | flask_gevent | 169.34 | 5.88 | 6.09 | 6.40 | 0 | 0.34 | 56.88 |
| io | 1 | fastapi_async | 171.34 | 5.81 | 6.11 | 6.59 | 0 | 0.27 | 53.63 |
| io | 16 | flask_gevent | 1,784.49 | 7.71 | 15.83 | 22.51 | 0 | 0.20 | 57.36 |
| io | 16 | fastapi_async | 1,849.33 | 7.52 | 14.63 | 18.81 | 0 | 0.12 | 54.04 |
| io | 64 | flask_gevent | 661.90 | 58.71 | 294.46 | 473.12 | 0 | 0.23 | 57.57 |
| io | 64 | fastapi_async | 703.00 | 54.58 | 280.58 | 414.07 | 0 | 0.15 | 54.10 |
| io | 256 | flask_gevent | 639.21 | 275.42 | 1,019.39 | 1,377.89 | 0 | 0.21 | 57.63 |
| io | 256 | fastapi_async | 689.26 | 251.82 | 927.29 | 1,276.44 | 0 | 0.14 | 54.17 |

[Raw HTTP trials](../benchmarks/results/http.json). Native FastAPI was about 14% faster for serial identity, slightly slower for identity at higher concurrency, and modestly faster for concurrent simulated I/O. At 256 clients the tails approach or exceed a second; the driver and connection pool contribute substantially. This is a bounded load/stress observation, not a demonstration of maximum server throughput. Use an independent generator and an open arrival-rate workload to evaluate production saturation and coordinated omission.

## Reliability findings from the original source

[Executable probe output](../benchmarks/results/original_probes.json) reproduced six defects using AST-selected original functions, injected transport failure boundaries and the original Lua script against real Redis. This is distinct from running a complete original broker-chaos matrix.

| Reproduced original behavior | Consequence | Successor behavior and evidence |
|---|---|---|
| Failed commit discards candidate bookkeeping | Completed progress cannot be retried as intended | Remove completed prefix only after broker ACK; failed-commit unit/runtime regressions |
| Coordinator assumes consecutive numeric offsets | Compacted/gapped offsets can stall progress | Track actual fetched order; numeric-gap regression |
| Later parse-error completion can pass an earlier unfinished record | Commit can skip unfinished work | Register fetched offsets before dispatch; completed-prefix regression |
| Default DLQ send failure is swallowed | Failed input may be treated as successfully handled | ACK-gated DLQ, transport budget, fail readiness/uncommitted regression |
| Single output returns before asynchronous broker result | Source completion can precede delivery confirmation | Await every output future; delayed/failed ACK tests |
| Complete aggregate is returned without exclusive claim/tombstone | Concurrent arrivals can execute duplicate joins | Atomic token claim, renewable lease, deterministic merge, completion tombstone tests |

Further fixes include policy wrappers applied outside worker registration, unsafe helper expression evaluation, truncated odd-width integer conversion, empty nested-path lookup, request-ID preservation on 500 errors, and distinct-source trace IDs in bulk output/DLQ. These are focused corrections with regressions, not a claim that every inherited utility or application bug has been eliminated.

## Live-service validation and recovery contract

The complete local suite passed **132 tests**: 120 unit/regression checks and 12 live-service checks, including 10,000-record stress. GitHub CI also passed Python 3.11/3.12/3.13 and the dedicated service-integration job. The final validation record is in [validation.json](../benchmarks/results/validation.json). Tests include:

| Test family | Executed behavior | Result / scope |
|---|---|---|
| Unit/regression | Settings, schemas, null models, policies, offset ordering/gaps, stale generations, output/DLQ/commit faults, startup cleanup and tracing | Passing local tests and CI; no live broker needed |
| Single/sync/bulk E2E | 101 inputs through each worker mode, real output and committed offsets | All outputs observed and source progress confirmed |
| Retry / poison E2E | Transient success, permanent exception and invalid JSON | Correct success and acknowledged diagnostic DLQ; attempts distinguish parse/application failure |
| Aggregate E2E | 80 inputs including duplicates, 20 correlations across two topics | 20 complete outputs under concurrent arrivals |
| Lease renewal | Work exceeds short claim lifetime | Ownership renewal prevents a competing ready claim |
| Stress | 10,000 messages, every tenth message transiently failing, eight jobs, 16 uncommitted records/partition | No missing/duplicate output; tested capacity bounds maintained |
| Graceful cancellation | Cancel blocked handler, restart same source group | Unfinished records replay and complete |
| SIGKILL without ledger | Kill after 20 output ACKs and before source commit, restart | 40 outputs with 20 unique IDs: expected at-least-once replay |
| SIGKILL with ledger | Same injection after completion ledger writes | 20 outputs with 20 unique IDs; remaining ACK-to-ledger crash window not eliminated |
| Broker outage | Pause dedicated broker, exhaust short ACK budget, unpause/restart | Readiness fails; all 20 input IDs recovered; duplicates permitted by contract |
| Real OTLP / Jaeger | HTTP → Kafka fanout → two workers → Redis join in four independent processes | One trace ID, linked spans, final output context verified |

Process crash and broker pause tests were run against the new runtime. The original received the same performance workloads and executable fault probes, not every identical process/chaos integration test. Do not infer a measured original-versus-new crash recovery time or failure probability from these asymmetric suites.

Important remaining boundaries:

- Kafka output, input commits, Redis writes and business effects are not one transaction. Producer idempotence is session-scoped protection, not exactly-once application processing.
- Optional source-coordinate ledger suppresses already recorded replay within its TTL; repeated business IDs at new offsets are independent records. A crash after output ACK but before ledger completion may duplicate output.
- Incomplete aggregate inputs commit after Redis storage. Redis AOF everysec can lose recent writes on machine failure; no cross-store atomic recovery is promised. Expired incomplete joins have no automatic expiry DLQ.
- Aggregate tombstone TTL defines the suppression window; reuse of correlation IDs can suppress legitimate new work. Claim loss cannot retract accepted Kafka output or user side effects.
- Handler attempts reset after process loss. Out-of-order concurrent completion can reorder downstream output, even though source commits remain prefix-safe.
- Cancellation fences framework publishing and progress; a running synchronous function may continue external effects. Total shutdown can exceed a single phase timeout.
- HTTP liveness stays available after ETL failure. Readiness returns 503; deployment supervision must restart or alert rather than assuming HTTP process exit.
- No multi-broker failover, TLS/SASL live test, Redis failover/data-loss test, long soak or production payload certification was executed.

## OTLP evidence in Jaeger

Latest archived verification: trace **`6b5415a5432febe48e50cfe476681f43`**, **13 spans**, **12 parent-child references**, services `qf-otlp-a, qf-otlp-b, qf-otlp-ingress, qf-otlp-merge`. Every returned span shared the HTTP trace ID; all references resolved within that trace; one `POST /publish` root existed; final Kafka output carried the same trace ID.

[Open the local Jaeger trace](http://localhost:26686/trace/6b5415a5432febe48e50cfe476681f43) while the current development container is running. [Verification JSON](../benchmarks/results/jaeger_verification.json) and [complete Jaeger response](../benchmarks/results/jaeger_trace.json) preserve the proof after ephemeral Jaeger storage resets.

Fanout branches share context. The aggregate handler is parented to the completing arrival; prior parts remain visible in the trace but their contexts are not stored as explicit multi-parent Redis join links. Distinct-root inputs are not merged into one trace ID. Bulk outputs preserving source IDs retain each source trace through a producer span linked to the batch; changed/new IDs fall back to the batch trace. Unit tests cover distinct trace IDs in bulk and DLQ. Outbound HTTP/manual publishing needs instrumentation or explicit header injection; see [Tracing.md](Tracing.md).

## Maintainability and extensibility

Physical source metrics, excluding tests/benchmarks/docs and including retained utilities:

| Implementation | Python files | Physical lines | Functions / methods | Largest module (lines, path) |
| --- | --- | --- | --- | --- |
| Original | 29 | 5018 | 250 | (1798, 'etl/framework_etl.py') |
| FastAPI | 35 | 3462 | 233 | (408, 'etl/framework_etl.py') |

These are descriptive counts, not a complexity or quality score. The original includes commented/dead sections; fewer lines do not establish correctness. The new runtime separates scheduling, commit coordination, processing, Redis aggregation, idempotency, settings, API compilation and tracing. This makes failure contracts easier to inspect and individual dependencies replaceable in tests. The explicit runtime dependency injection used in unit tests improves reviewability without requiring a service container.

| Concern | Original | Successor |
|---|---|---|
| Concurrency model | gevent patching plus thread executors and synchronous clients | One loop owns consumer state; async broker/Redis; bounded sync adapters |
| Lifecycle | Global/import-time configuration and background startup coupling | FastAPI lifespan, explicit ownership, startup propagation, bounded drain and flush |
| API schemas | Flask/RESTX dynamic routes and model translation | Compile once, Pydantic/OpenAPI, typed paths, construction-time validation, null-model support |
| Config | Application-global Config assumptions | Validated settings; explicit/env/dotenv precedence plus legacy bridge |
| Failure testing | Limited original coverage observed | Focused unit regressions plus real broker/Redis, SIGKILL, broker pause and collector checks |
| Observability | Existing tracing/logging helpers with inconsistent failure visibility | Preserved helper conventions plus readiness, scoped Prometheus, request/trace IDs and archived propagation proof |
| Extension path | Extend central ETL path or sync helpers | Add native routes/dependencies, new async workers, policies and separately testable processing/storage components |

Compatibility is practical, not binary drop-in: install separately, replace WSGI startup, remove gevent monkey patching, migrate Flask globals/extensions, await async Redis/Kafka helpers, and adapt DLQ consumers to envelopes. Legacy worker decorators, function signatures, uppercase Config fields, endpoint JSON concepts and tracing methods are retained. See [Migration.md](Migration.md) for supported keys and deliberate removals.

Remaining maintenance risks include module-level worker registration, a process-owned OpenTelemetry provider, central scheduler complexity and retained APDU domain utilities. Independent apps in one process need registry isolation and one tracing configuration. The current API mapping format intentionally covers JSON handlers rather than all Flask extensions/forms. Tests provide confidence in exercised behavior, not a bug-free guarantee or quantified complete coverage. Dependency ranges need periodic compatibility verification and deployment locking.

## Reproduction and artifact index

Install separate environments, start the dedicated compose stack, and run suites sequentially. Complete commands and interpretation are in [benchmarks/README.md](../benchmarks/README.md).

```bash
pip install -e '.[dev,tracing]'
docker compose -p qf-fastapi-test up -d --wait
QF_INTEGRATION=1 QF_CHAOS=1 QF_JAEGER=1 QF_STRESS_MESSAGES=10000 python -m pytest -q
python benchmarks/original_probes.py
python benchmarks/compare_pipelines.py --messages 1000 --repeats 3 --concurrency 8
python benchmarks/compare_pipelines.py --messages 5000 --repeats 3 --concurrency 32 \
  --engines async,sync,original_ack --workloads bulk \
  --output benchmarks/results/pipelines_bulk_tuned_rerun.json
python benchmarks/compare_http.py --requests 1000 --repeats 3
ruff check .
ruff format --check .
python -m build
```

Original benchmark processes require the sibling framework's `.venv` with its dependencies and gevent. `--original-root` can override its location. Changing defaults or using another checkout requires recording new source revisions and environment snapshots. The original raw datasets are retained; do not overwrite them when comparing another machine.

Artifact index: [final pipelines](../benchmarks/results/pipelines.json), [initial scale](../benchmarks/results/pipelines_scale.json), [tuned bulk](../benchmarks/results/pipelines_bulk_tuned.json), [HTTP](../benchmarks/results/http.json), [original defect probes](../benchmarks/results/original_probes.json), [validation](../benchmarks/results/validation.json), [Jaeger proof](../benchmarks/results/jaeger_verification.json). Specifications, architecture and operations are maintained in this repository alongside the report. The canonical comparison is also copied to the requested parent workspace location.

## Design references

Primary documentation supporting implementation choices, separate from local benchmark evidence: [aiokafka manual commits](https://aiokafka.readthedocs.io/en/stable/consumer.html#manual-vs-automatic-committing), [aiokafka producer idempotence](https://aiokafka.readthedocs.io/en/stable/producer.html#idempotent-produce), [FastAPI lifespan](https://fastapi.tiangolo.com/advanced/events/), [OpenTelemetry Python propagation](https://opentelemetry.io/docs/languages/python/propagation/), and [Jaeger 1.62 deployment/OTLP](https://www.jaegertracing.io/docs/1.62/deployment/).
