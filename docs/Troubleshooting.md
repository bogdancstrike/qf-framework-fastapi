# Troubleshooting

Start with `/health/ready`, process logs and broker-reported consumer-group offsets. HTTP liveness alone does not establish that workers are processing. In worker-only mode there is no HTTP diagnostics server; inspect process exit status and logs through your supervisor.

## Startup fails or readiness stays 503

| Symptom | Check / action |
|---|---|
| ETL settings validation error | Supply `worker_modules`, `kafka_bootstrap_servers` and `consumer_name`, or set `enable_etl=False` for HTTP-only mode |
| Missing endpoint JSON | Disable dynamic endpoints for native routes, or set the file path relative to `app_root` |
| Duplicate worker/topic error | Ensure unique names and one input owner per process; look for unrelated imported worker modules |
| Kafka connection / advertised-address error | Match host `localhost:29092` versus compose-network `kafka:9092`; inspect broker advertised listeners |
| Redis connection error | Host examples use `redis://localhost:26379/0`; a join or source ledger requires Redis |
| Missing tracing exporter | Install `.[tracing]`; supply `QSINT_OTLP_ENDPOINT` when enabling tracing |
| Readiness changed from 200 to 503 | Inspect terminal transport/commit/runtime errors, restore the dependency and restart the failed runtime |

The framework retries startup within configured budgets and cleans up on failure. Repeated restarts with the same invalid settings do not fix the cause. Constructor arguments override environment values; inspect your application construction if changing an environment setting has no effect. The full pipeline demo reads process environment and deliberately ignores `.env`.

## Input arrives but output does not

1. Confirm the producer topic matches a registered `topics_in` and the consumer group has an assignment. Provision the output and DLQ topics too.
2. Inspect input JSON: Kafka workers require objects. Check the DLQ for malformed input, missing aggregation keys or exhausted handler attempts.
3. Check whether the handler returned `None` or an empty list: these intentionally emit no output.
4. For joins, verify **all** configured input topics delivered the same correlation key, within retention, under the same group/worker namespace. Reusing a recently completed ID hits its tombstone.
5. Inspect inflight jobs, pending/uncommitted records, rate policies and circuit state. A slow job or downstream ACK can prevent committed progress even when later jobs have finished.
6. If the runtime failed readiness, restore the dependency and restart. An HTTP process can remain alive after terminal ETL failure.

For the local example, observe committed progress with:

```bash
docker compose -p qf-fastapi-test exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group qf.demo.workers
```

Use your actual group if overridden. Joining the same group with a debugging consumer steals partitions; prefer a separate diagnostic group and avoid committing application offsets manually.

## Duplicate output or repeated business effects

At-least-once replay is expected after some crashes. Record source topic/partition/offset, business ID and process restart timing. Determine whether you saw a replay of the **same source offset** or a new input carrying the same business ID; the optional source ledger only targets the first case.

Even with the ledger, a crash between output ACK and ledger completion, TTL expiration, lost Redis state or changed group/worker namespace can permit replay. A partial multi-topic output acknowledgement followed by transport retry can also repeat publication. Idempotent Kafka producers do not make a restarted business operation exactly once.

Use the downstream storage/provider's atomic idempotency mechanism for business effects. Do not resolve duplicates by skipping arbitrary source offsets: that can lose unfinished work. See the [idempotency scope table](Workers.md#choosing-idempotency-boundaries) and [SIGKILL tests](../tests/test_crash_recovery.py).

## Join never completes or has unexpected fields

Check the exact aggregation path, such as `order.id`; top-level `id` and `order.id` are different contracts. A join retains one part per topic/correlation, so multiple arrivals from one branch do not replace a missing second branch. A later same-topic part can replace an earlier one before claim. Scalar field conflicts follow `topics_in` order, regardless of arrival order.

Incomplete parts can expire without DLQ. Increasing Kafka retention does not extend Redis retention, and changing group or worker names isolates previously stored parts. If Redis lost committed incomplete parts, replaying only the missing branch may be insufficient; reconstruct the whole business correlation under a deliberate replay policy. Completion suppression also expires; reusing a business ID indefinitely is unsafe.

## Throughput falls or memory grows

Observe these together:

| Signal | Interpretation / next check |
|---|---|
| High inflight jobs and long job duration | External latency, local retries or expensive handler work; profile the real integration |
| Low inflight with high pending | Rate/circuit admission, incomplete bulk batches, partition availability or scheduling |
| High uncommitted with low queued work | Completed work blocked behind a predecessor, slow commits or broker errors |
| Increasing event-loop lag | Blocking work inside async handlers, CPU pressure or overloaded callbacks |
| Lag rises while retries/errors rise | Dependency saturation; increasing concurrency may worsen it |
| RSS rises with bounded record count | Large payloads, batch copies, application caches/pools or retained user tasks |

Bounds count records/jobs, not bytes. The generator, broker and Redis consume resources outside worker RSS. A bounded worker queue does not bound an application's unrelated caches or detached tasks. Async user code runs on one event loop; avoid blocking clients and CPU-heavy loops. Sync user code uses threads but still needs bounded external work.

Change one setting at a time and repeat the same workload. Inspect `fetch_max_wait_ms`, batch age/size, commit cadence and downstream connection limits before raising buffers. The published speedups are workload-specific: stock fire-and-forget output and different concurrency can change the ranking. See the [report](qf_flask_vs_qf_fastapi_report.md) for measured exceptions.

## Jaeger shows no spans or disconnected traces

1. Install tracing extras, enable tracing and use the OTLP **gRPC** endpoint. The local collector is `http://localhost:24317`; `26686` is its UI, not ingestion.
2. Confirm `QSINT_OTLP_INSECURE=true` only for the local plaintext endpoint. Check collector reachability from the actual process/container.
3. Allow batch export delay, then gracefully shut down to flush. SIGKILL can lose unexported spans.
4. Preserve W3C Kafka headers. Framework output and `KafkaClient.put_message()` inject them; arbitrary external publishers must call `inject_trace_headers()` themselves.
5. Start the complete job under one HTTP/root context. Independent console-producer messages have no shared originating trace unless you explicitly supply one.
6. Keep bulk output IDs stable for source lineage. A join parents processing to the completing arrival; it does not unify independent trace roots retroactively.

Only one process-owned tracing provider is supported per process. One-process examples display one service; the real collector test intentionally uses four processes/services. Run `QF_JAEGER=1 python -m pytest tests/test_otlp_jaeger.py -v` to test collector storage and parent references, rather than relying solely on headers. See [Tracing.md](Tracing.md).

## Understand and inspect the DLQ

Diagnostic envelopes are not application messages. They carry original bytes in `raw_base64`, source coordinates/key, worker, attempt count and error type. For an envelope saved as `dlq.json`, inspect it without publishing anything:

```python
import base64
import json
from pathlib import Path

envelope = json.loads(Path("dlq.json").read_text())
raw = base64.b64decode(envelope["raw_base64"], validate=True)
print(envelope["source"], envelope["error"], envelope["attempts"])
print(raw.decode("utf-8", errors="replace"))
```

Fix the underlying fault and validate decoded content before an explicit replay. Malformed original bytes still cannot be parsed as JSON. Aggregate envelopes may include a merged snapshot; recovering a join requires understanding the branch and tombstone state. Republishing creates new offsets, so source-coordinate deduplication does not suppress it. See [operations](Operations.md#failure-and-retry-behavior) for replay boundaries.

## Tests hang or a benchmark times out

Check service readiness, matching advertised addresses, and competing consumers/chaos tests first. A benchmark timeout alone does not identify the bottleneck. Worker results include phase/stage, observed unique outputs and expected/observed source offsets to distinguish output collection from commit stalls. Preserve these diagnostics and success/attempt counts.

Run a small functional test before a 100,000-message comparison. Use unique namespaces for tests, and keep failure injection separate from measurements. For a useful bug report include framework commit, Python/dependency versions, redacted settings, topic partitions/group, a minimal input, logs with trace/request IDs and the exact command. Keep credentials and sensitive payloads out of reports.
