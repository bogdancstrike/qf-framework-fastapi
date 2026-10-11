# Worker contracts and cookbook

For a runnable multi-stage application, start with the [pipeline example](../examples/pipeline/README.md). This guide explains how to adapt that pattern safely.

## Registration and discovery

`@kafka_handler` and `@kafka_aggregator` register a function when its module is imported. `worker_modules` lists the modules the runtime imports at startup. Names must be unique, and an input topic has one worker owner per process. A handler may consume several topics and publish to several topics; an aggregator requires at least two input topics.

All registered workers in a process belong to its runtime registry. Use separate processes/applications when you need isolated worker sets or different consumer groups; importing unrelated decorated modules can register additional workers. Each serving process starts its own runtime. Replicas in the same group share partitions, while different groups each consume their own copy of a topic.

## Inputs, metadata and returns

```python
from framework.decorators import kafka_handler


@kafka_handler(
    name="tag",
    topics_in=["documents.in"],
    topics_out=["documents.tagged"],
    metadatas={"schema_version": 1},
    max_workers=8,
)
async def tag(message, consumer_name, metadatas):
    return {**message, "schema_version": metadatas["schema_version"]}
```

| Argument / return | Contract |
|---|---|
| `message` | Decoded JSON object; a list of objects for bulk handlers; merged object for aggregators |
| `consumer_name` | Configured consumer group/application name |
| `metadatas` | Your decorator's static dictionary, copied per invocation; not Kafka record coordinates |
| Dict return | Publish this object to **every** `topics_out` topic |
| List of dicts | Publish each object to every output topic; useful for expansion and bulk results |
| `None` | Successful filtering, with no output; source may complete |
| Exception | Handler retry, then diagnostic DLQ after exhaustion |

Workers must return JSON-compatible values. Serialization/type failures are application failures and follow handler retry/DLQ policy. Kafka input must be a JSON object; malformed JSON, arrays and scalar values go to diagnostic DLQ before user code. Missing/null/empty `id` is filled deterministically from source coordinates. Existing `0` and `false` IDs are retained, but strings are usually a clearer application contract.

Preserve `id` across pipeline stages. Framework publications use its string value as the Kafka key when present. This provides partition affinity; it does not serialize concurrent work with the same key or create business deduplication. Give fanout branches distinct field names to avoid accidental join conflicts.

## Async I/O with an owned, reusable client

Install `httpx` as an application dependency. A complete module saved as `myapp/enrichment.py` can combine a worker and its resource lifespan:

```python
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx

from framework.app import FrameworkApp
from framework.config import FrameworkSettings
from framework.decorators import kafka_handler

resources = SimpleNamespace(http=None)


@kafka_handler(
    name="customer-enrichment",
    topics_in=["orders.validated"],
    topics_out=["orders.enriched"],
    max_workers=8,
)
async def enrich(message, consumer_name, metadatas):
    response = await resources.http.get(f"/customers/{message['customer_id']}")
    response.raise_for_status()
    return {**message, "customer": response.json()}


settings = FrameworkSettings(
    worker_modules=["myapp.enrichment"],
    kafka_bootstrap_servers="localhost:29092",
    consumer_name="customer-enrichment-v1",
    enable_dynamic_endpoints=False,
)
framework = FrameworkApp(settings)
app = framework.build_app()
framework_lifespan = app.router.lifespan_context


@asynccontextmanager
async def lifespan(application):
    async with httpx.AsyncClient(base_url="http://localhost:8001", timeout=5) as client:
        resources.http = client
        try:
            async with framework_lifespan(application):
                yield
        finally:
            resources.http = None


app.router.lifespan_context = lifespan
```

Run `uvicorn myapp.enrichment:app --workers 1`. Provide your customer service and provision the topics. The client exists before workers start and closes after they stop. Reuse pools rather than creating connections for every message. Add authentication, response validation and business-specific handling of permanent errors to suit your service.

Use native async database/HTTP clients. `requests`, `time.sleep()` and heavy computation inside `async def` block all tasks on that event loop. Existing blocking code can remain a synchronous decorated function: the runtime uses a bounded per-worker executor with tracing context propagation. CPU-heavy tasks generally need a process/service boundary. Running sync code in threads does not make its external side effects cancellable.

Worker-only `FrameworkApp.serve()` uses a separate runner rather than ASGI lifespan. The composed resource example above targets Uvicorn; if you build a worker-only application, arrange equivalent resource ownership around its async runner.

## Concurrency, backpressure and ordering

`max_workers` limits active jobs **per worker per process**. A bulk job contains many records, while `pending_max_per_tp` bounds individual fetched records awaiting commit. A second replica adds another process's job capacity, subject to Kafka partition assignment and downstream limits.

The runtime commits only a completed prefix of actual fetched records. A later job may finish and publish first, then wait behind an earlier unfinished record before its source offset can be committed. Numeric gaps in fetched offsets are valid. Safe source commits therefore do **not** imply ordered output or ordered external side effects.

When strict per-key business ordering is required, design explicit serialization/fencing around that key. Lowering concurrency can reduce reordering, but multiple partitions, retries, replicas and downstream consumers still need a deliberate ordering contract. There is no built-in global ordering guarantee.

Increase concurrency gradually while inspecting job duration, inflight jobs, consumer lag and downstream errors. A process with many active jobs can still be bottlenecked on output acknowledgements or a slow predecessor blocking committed progress.

## Bulk size, age and failures

```python
@kafka_handler(
    name="bulk-tag",
    topics_in=["bulk.in"],
    topics_out=["bulk.out"],
    bulk_mode=True,
    batch_size=32,
    batch_timeout_ms=25,
    max_workers=8,
)
async def bulk_tag(messages, consumer_name, metadatas):
    return [{**message, "processed": True} for message in messages]
```

Batches stay within a source partition. Size is a maximum, and the oldest queued record triggers a partial-batch age flush; occupied worker slots and admission policies can delay dispatch. Do not assume every batch has exactly 32 messages.

Handler retry reruns the batch from a fresh deep copy. On exhaustion, each accepted source record receives a diagnostic DLQ entry. If your external bulk operation partially succeeds before raising, a retry can repeat those effects. Use per-item idempotency and choose an explicit partial-failure strategy for your integration. Returning fewer objects intentionally completes the whole input batch once returned outputs are acknowledged.

Stable input/output IDs preserve per-source trace lineage. New or changed IDs use the batch context; see [tracing](Tracing.md#fanout-joins-and-bulk).

## Fanout and correlation joins

Multiple output topics fan out the **same returned objects**; the decorator is not a conditional router. To route conditionally, use a downstream filtering stage or implement an application-owned publisher with an explicit delivery contract.

```python
from framework.decorators import kafka_aggregator


@kafka_aggregator(
    name="order-join",
    topics_in=["orders.pricing", "orders.inventory"],
    topics_out=["orders.ready"],
    aggregate_by="order.id",
    aggregator_timeout_sec=600,
    max_workers=8,
)
async def ready(message, consumer_name, metadatas):
    return {**message, "ready": True}
```

The join stores one part **per input topic per correlation**, not an arbitrary number of events from each topic. Before a claim, another arrival on the same topic replaces that topic's retained part. The merged object follows `topics_in` order: later topics win scalar conflicts; nested dictionaries merge recursively. Include all required application fields in the branch contract.

Redis keys include group, worker and correlation. Use stable group/worker names when restarting and fresh correlation IDs for independent operations. Changing names creates a different join namespace. A renewable lease protects processing ownership; parts remain until output/DLQ acknowledgement and completion. A tombstone suppresses repeated completed correlations until expiration.

Incomplete parts expire according to the aggregation timeout, refreshed by writes and active claim renewal. Expiration has no automatic DLQ or missing-branch notification. Select a timeout exceeding expected branch delay/recovery and monitor business completion separately. Redis persistence/failover determines whether already-committed incomplete parts survive infrastructure failure.

## Retry, rate and circuit policies

```python
from framework.decorators import circuit_breaker, rate_limit, retry_to_dlq


@retry_to_dlq(max_attempts=3, dlq_topic="payments.dlq")
@rate_limit(rps=20, burst=40)
@circuit_breaker(failures=5, reset_sec=30)
@kafka_handler(
    name="payment-stage",
    topics_in=["payments.in"],
    topics_out=["payments.checked"],
    max_workers=8,
)
async def payment_stage(message, consumer_name, metadatas):
    return {**message, "checked": True}
```

These decorators attach runtime metadata; directly calling the Python function does not run the Kafka retry/admission/DLQ machinery. Decorator order is supported in either direction. `max_attempts` includes the first attempt. Handler retries are local, use fresh copies and full jitter, and reset after a process restart. Legacy `retry_count_field` is accepted as metadata; retry bookkeeping is not persisted in the business payload.

Rate limits count admitted **jobs**, including bulk jobs, per worker process. They are not distributed quotas; three replicas can admit roughly three times the configured process rate. Circuit state is also local. A job that exhausts handler attempts and reaches DLQ counts as failure for circuit recovery; short transient failures resolved within a successful job do not each count as separate failed jobs. Function-level `call_retry`, `call_rate_limit` and `call_circuit_breaker` wrap actual calls and have their own arguments; do not assume the same semantics as worker metadata decorators.

There is no generic handler execution deadline setting. Set deadlines on external calls or wrap your own operation in `asyncio.timeout()`. Cancellation should propagate; do not swallow `CancelledError`. Output/Redis transport retries have a separate budget and do not normally rerun successful user code, but partial acknowledgements can still produce duplicates when a multi-output transport attempt is retried.

## Choosing idempotency boundaries

| Mechanism | Protects | Does not protect |
|---|---|---|
| Idempotent Kafka producer | Supported duplicate sends within its producer session/protocol | Business effects or a fresh process publishing the same work |
| Optional source ledger | Completed replay of a non-aggregator source topic/partition/offset within TTL | Same business ID at a new offset; ACK-to-ledger crash window |
| Aggregate lease/tombstone | Concurrent join ownership and repeated completed correlation within timeout | Durable indefinite uniqueness; lost Redis state; independent reused IDs |
| Application idempotency key | Business operation according to your storage/provider contract | Anything beyond that contract's atomicity and retention |

Enable the source ledger with `QSINT_IDEMPOTENCY_ENABLED=true`, provision Redis and choose `QSINT_IDEMPOTENCY_TTL_SEC` longer than your expected replay horizon. For payments, writes or notifications, use the downstream system's atomic idempotency capability with a stable business operation ID. The framework does not provide a transaction spanning Kafka, Redis and your database. See [delivery and recovery](Operations.md#delivery-contract) before choosing replay behavior.
