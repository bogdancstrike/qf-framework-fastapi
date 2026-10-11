# Utilities, execution leases and resource ownership

The utility audit covers `SingleExecute`, common decorators, Redis/Kafka helpers, byte conversion and APDU handling. The [worker guide](Workers.md) defines the message delivery contract; utility decorators do not replace it.

## SingleExecute: one owner with a finite lease

Use this for competing maintenance jobs or coordination around an explicitly idempotent operation. It uses a shared Redis key, a unique ownership token and renewable expiry. The caller owns the Redis client and must close it in its application lifespan.

```python
from redis.asyncio import Redis
from framework.decorators.single_execute import SingleExecute

redis = Redis.from_url("redis://localhost:26379/0", socket_timeout=2)

@SingleExecute(
    lock_id="refresh-catalog",
    redis_client=redis,
    run_ttl_sec=30,
    renew_every_sec=5,
    cooldown_sec=60,
    cooldown_on_error_sec=0,
)
async def refresh_catalog():
    return {"refreshed": True}
```

Run [the complete competing-call example](../examples/single_execute.py) with `python -m examples.single_execute`. Five async callers compete for one key; one executes and the others skip during the lease/cooldown window.

| Option / behavior | Meaning |
|---|---|
| `mode="auto"` | Async function requires `redis.asyncio.Redis`; sync function requires synchronous Redis. Explicit `asyncio`/`thread` modes must match. |
| `run_ttl_sec`, `renew_every_sec` | Positive finite lease with renewal strictly below TTL; millisecond precision permits fractional seconds. |
| `cooldown_sec` | Suppression after successful completion. Zero normally releases immediately. |
| `cooldown_on_error_sec` | Error/cancellation suppression; defaults to the normal cooldown. Choose zero when immediate retry is appropriate. |
| `block`, `block_timeout_sec` | Optional wait for ownership; use a finite timeout when blocking. |
| `raise_if_not_acquired` | Raise on contention instead of returning `on_skip_return`, which defaults to `None`. |
| Ownership loss | Completion verifies the token atomically, even if work finishes before the first renewal. An old owner cannot remove or extend the new owner's key. |
| Cleanup failure | Preserve the original application error; log failed cleanup and allow the lease to expire. |

**Worker caveat:** returning `None` is successful filtering and allows Kafka input completion. A skipped `SingleExecute` call returning `None` can therefore intentionally discard that input's processing. For critical worker work, use `raise_if_not_acquired=True`, a suitable bounded wait/retry policy and business idempotency. Exhausted handler attempts route input to DLQ; inspect/replay it rather than assuming an endless retry queue. This decorator is usually better suited to periodic maintenance than per-message deduplication.

A killed owner leaves a key until its TTL expires. A paused owner can resume after expiry while another process owns the key. Token checks protect Redis ownership; they cannot undo external effects. The token is not a monotonic business fencing sequence. Use a unique business key/constraint or downstream idempotency API for payments, inserts and other durable effects. See [Redis distributed lock guidance](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/).

Sync renewal uses a background thread; configure bounded Redis socket/connect timeouts. A running sync handler cannot be forcibly cancelled by asyncio. Unknown `SingleExecute` keyword arguments now fail immediately instead of being silently ignored.

## Common decorators

`retry`, `circuit_breaker` and `rate_limit` retain sync/async wrapping and refresh the worker registration when placed outside the worker decorator. Avoid stacking many independent retry layers: their attempt budgets multiply.

The function circuit breaker is process-local. A generation fence stops an older in-flight success from closing a newer open circuit. Cancellation of a half-open probe releases its probe slot without treating cancellation as an ordinary application failure. The worker scheduler also has its own admission policy; keep its purpose separate from a decorated downstream-call circuit.

The default function rate limiter is local to one process. An explicitly shared supported storage backend can share limits; it is not automatically a deployment-wide worker quota. Async wrappers move synchronous limiter storage calls to a thread. Worker `max_workers` and scheduler admission limits apply per process, so four replicas multiply potential downstream pressure.

## Kafka and Redis helpers

Use `async with KafkaClient(...)` for acknowledged publication. It injects tracing headers and uses an idempotent producer. Double entry is rejected; exit closes and clears the producer, and a later context can create a fresh producer. Sending outside an active context fails clearly. JSON values must be finite: `NaN`/infinity are rejected rather than emitted as nonstandard JSON.

`RedisUtils` owns its pool and closes it on context exit or failed startup ping. Every method must be awaited. `retry_on_timeout=True` configures one additional client transport retry; the default configures none. A timeout can follow a successful server-side write, so do not enable blind retries of non-idempotent increments without accounting for that ambiguity. Framework claim/completion retries use stable tokens and idempotent Lua receipts instead.

## The instances package

Both the original and successor `framework.instances` packages are empty namespaces. There is no implemented singleton registry or resource lifecycle hidden there. Keep clients in the application lifespan and `app.state`, or use explicit dependency injection. Do not create loop-bound clients at import time or share an asyncio client across event loops/processes.

The [pipeline app](../examples/pipeline/app.py) composes the framework lifespan with an application-owned `KafkaClient`, then exposes it as `request.app.state.ingress`. Its workers and ingress use separate producer lifecycles. Worker resource injection beyond explicit application composition remains a proposed extension.

## Byte and APDU helpers

The retained smart-card utilities now cover short and extended APDU round trips, payload lengths 1–65,535, correct optional `Le`, malformed-length rejection and length boundaries 255/256/65,535. These fixes do not certify every smart-card application protocol.

- Byte-string helpers default to Latin-1 for a reversible mapping of byte values 0–255. Non-byte Unicode raises `UnicodeEncodeError`; explicitly select UTF-8 in helpers accepting `encoding` for text. The old mixed character conversion could silently corrupt values above 255.
- `loi` counts digits in base `lim + 1`; exact full-byte boundaries are correct and not artificially limited to three bytes.
- `llh(..., apdu=True)` emits valid short/extended command lengths and rejects bodies over 65,535 bytes. BER length output preserves the existing long-form convention and is not a general DER canonicalizer.
- `apdu_expand` parses short/extended cases 1–4 and trailing `Le`; malformed packets return `(None, None, None)`. `apdu_compact` validates the four-byte header and keeps `Lc`/`Le` formats consistent.
- `lower_rx` normalizes string keys while preserving query/payload values regardless of their key's original casing, and retains non-string keys. `nth_obj` retains legitimate zero/false values.

Other existing helpers retain intentional compatibility: `getor` selects a truthy fallback and `clean_list` filters falsy entries. Choose explicit checks when zero/false must be retained. Extended length semantics follow [Oracle's Java Card APDU format](https://docs.oracle.com/en/java/javacard/3.1/guide/extended-apdu-format.html).

Run the focused audit with:

```bash
python -m pytest tests/test_utilities_hardening.py tests/test_resource_helpers.py -q
```
