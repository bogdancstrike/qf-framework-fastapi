import asyncio
from dataclasses import replace

import fakeredis.aioredis

from framework.redis.aggregation import AggregationStore
from framework.etl.processing import Processor
from framework.observability import Metrics
from conftest import Producer, record


async def test_claim_merge_order_concurrency_and_tombstone(settings, spec):
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = AggregationStore(settings, redis)
    spec = replace(spec, kind="aggregator", topics_in=["a", "b"], aggregate_by="id")
    first = await store.claim(spec, "id", "b", {"v": "b"})
    assert first.status == "stored"
    claims = await asyncio.gather(*(store.claim(spec, "id", "a", {"v": "a"}) for _ in range(20)))
    ready = [claim for claim in claims if claim.status == "ready"]
    assert len(ready) == 1 and ready[0].payload == {"v": "b"}
    assert sum(c.status == "busy" for c in claims) == 19
    await store.finish(ready[0], 60)
    assert (await store.claim(spec, "id", "a", {})).status == "done"
    await redis.aclose()


async def test_release_preserves_parts_and_token_fences_finish(settings, spec):
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = AggregationStore(settings, redis)
    spec = replace(spec, topics_in=["a", "b"])
    await store.claim(spec, "id", "a", {"a": 1})
    first = await store.claim(spec, "id", "b", {"b": 2})
    await store.release(first)
    second = await store.claim(spec, "id", "b", {"b": 2})
    assert second.status == "ready"
    try:
        await store.finish(first, 60)
        assert False, "Stale claim must fail"
    except RuntimeError:
        pass
    assert await redis.get(second.keys[1]) == second.token
    await store.release(second)
    await redis.aclose()


async def test_source_dedup_skips_replayed_record(settings, spec):
    settings.idempotency_enabled = True
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = AggregationStore(settings, redis)
    producer = Producer()
    processor = Processor(settings, producer, Metrics(), aggregation=store)
    await processor.handle(spec, [record()], lambda: True)
    await processor.handle(spec, [record()], lambda: True)
    assert len(producer.sent) == 1
    await redis.aclose()
