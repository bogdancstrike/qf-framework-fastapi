# Original framework analysis

Reviewed source: sibling `qf_framework`, revision `d54b3acae4b902e8715455bc9464ae11565ba739`.

The original supplies useful decorators, deterministic one-worker-per-topic routing, per-worker pools, strict bulk timeout flushing, Redis joins and explicit Kafka trace helpers. Its conceptual application model is retained. The migration changes resource ownership, transport clients and delivery bookkeeping rather than replacing the worker abstraction.

## Reproduced delivery defects

`benchmarks/original_probes.py` executes the actual original coordinator and send-policy functions selected by AST, injecting transport failures at their boundaries. The original Redis Lua script also runs against real Redis. All six probes reproduced the defect; raw results are in `benchmarks/results/original_probes.json`.

| Original path | Root cause | New behavior |
|---|---|---|
| `CommitCoordinator.try_commit` | Removes completed offsets and advances its cursor before a commit succeeds; `_commit_offsets` swallows failure | Retain candidate and fetched records until broker acknowledgement |
| Coordinator sliding window | Requires each numeric offset; compacted offsets such as 0, 2 stall at 1 | Advance through actual fetched record sequence |
| Malformed later record | Calls `mark_done` before earlier queued records initialize partition tracking | Register every admitted fetched record before processing; poison input requires ACKed DLQ |
| `_handle_retry_to_dlq` without policy | Ignores failed fallback DLQ send and returns True | Transport exhaustion fails runtime; source remains uncommitted |
| Single/aggregate output | Returns success after producer enqueue with `output_sync=False` | Await all output acknowledgements before completion |
| Aggregation script | Every arrival after all topics exist returns complete state; no claim or tombstone | Atomic claim, renewable ownership, deterministic merge and completion tombstone |

## Additional source findings

- The runner's shutdown method only emits a warning. The runtime shuts pools with `wait=False` and closes clients while jobs may still run.
- No rebalance listener clears queues/completions or fences late work. Consumer operations are split between poll and worker threads behind a lock, which serializes dispatch/commit under load.
- ETL imports application `config.Config` and constructs Redis eagerly. Even non-aggregating workers load the Redis script during startup.
- Consumer group ID comes from Config rather than the `consumer_name` argument.
- Failed output forwarding can return False without scheduling replay or terminating the loop; the gap can stall commits while later completions accumulate.
- Pending queues are bounded but completed offsets behind a stuck predecessor are not bounded by that pending limit.
- Retry republishing mutates the worker's input and immediately requeues it, allowing retry storms and exposing attempt counters to business payloads.
- Call circuit/rate wrappers are sync-only and can incorrectly treat a returned coroutine as completed work. Outer call decorators do not update an already registered function.
- Logging changes Python's global logger class and installs handlers at import. HTTP setup globally suppresses insecure TLS warnings.
- Tracing setup imports the SDK eagerly, relies on environment independently of runner flags, and does not flush spans in shutdown.
- Packaging dependency lists conflict, include three Kafka distribution names, and omit required runtime dependencies in project metadata.
- README describes a configurable `before` commit strategy, but the reviewed executable loop always constructs the coordinator and does not read `KAFKA_COMMIT_STRATEGY`. The documented tests and PoC app are absent in this checkout.
- Utility expression evaluation, integer conversion on odd-width hex values, empty nested paths and zero error cooldown have regression fixes in the new project.

## Performance implications

The original's fire-and-forget path can release a worker slot rapidly, producing strong trivial-work throughput but exposing an acknowledgement/commit failure window. Comparing it only to a safe ACK-gated successor obscures the tradeoff. Benchmarks include stock original behavior and an explicitly marked benchmark-only single-output ACK gate. The native async runtime frees worker threads from broker waits, wakes the scheduler on completion, batches output enqueues before acknowledgement, avoids repeated input decode/re-encode, and removes cross-thread consumer locking. Actual measurements, including slower cases and limits, are in the parent comparison report.
