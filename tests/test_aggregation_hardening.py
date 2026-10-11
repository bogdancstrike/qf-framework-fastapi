import asyncio
import json
from dataclasses import replace

import fakeredis.aioredis
import pytest

from framework.decorators import kafka_handler
from framework.decorators.kafka_workers import all_workers, compute_aggregate_key, register_worker
from framework.etl.processing import Processor
from framework.observability import Metrics
from framework.redis.aggregation import AggregationStore
from conftest import Producer, record


async def test_bulk_source_ledger_skips_completed_subset_and_counts_it(settings, spec):
    settings.idempotency_enabled = True
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = AggregationStore(settings, redis)
    producer, metrics = Producer(), Metrics()
    processor = Processor(settings, producer, metrics, aggregation=store)
    rows = [record(i, json.dumps({"id": str(i)}).encode()) for i in range(5)]
    try:
        await processor.handle(spec, rows[:1], lambda: True)
        await processor.handle(replace(spec, bulk_mode=True), rows, lambda: True)
        await processor.handle(replace(spec, bulk_mode=True), rows, lambda: True)
        assert len(producer.sent) == 5
        assert {json.loads(value)["id"] for _, value, _ in producer.sent} == {
            str(i) for i in range(5)
        }
        assert metrics.processed.labels(spec.name, "duplicate")._value.get() == 6
        assert metrics.idempotency.labels(spec.name, "skipped")._value.get() == 6
    finally:
        await redis.aclose()


async def test_concurrent_replay_waits_for_source_owner_then_skips(settings, spec):
    settings.idempotency_enabled = True
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    entered, release = asyncio.Event(), asyncio.Event()

    async def work(message, *_):
        entered.set()
        await release.wait()
        return message

    producer = Producer()
    processor = Processor(
        settings, producer, Metrics(), aggregation=AggregationStore(settings, redis)
    )
    worker = replace(spec, fn=work)
    first = asyncio.create_task(processor.handle(worker, [record()], lambda: True))
    second = None
    try:
        await entered.wait()
        second = asyncio.create_task(processor.handle(worker, [record()], lambda: True))
        await asyncio.sleep(0.01)
        assert not second.done()
        release.set()
        await asyncio.wait_for(asyncio.gather(first, second), 2)
        assert len(producer.sent) == 1
    finally:
        release.set()
        for task in (first, second):
            if task and not task.done():
                task.cancel()
        await asyncio.gather(*(t for t in (first, second) if t), return_exceptions=True)
        await redis.aclose()


async def test_lost_redis_replies_do_not_repeat_handler_or_strand_own_claim(settings, spec):
    settings.idempotency_enabled = True
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)

    class AmbiguousStore(AggregationStore):
        claims_lost = finishes_lost = False

        async def claim_many(self, requests):
            result = await super().claim_many(requests)
            if not self.claims_lost:
                self.claims_lost = True
                raise ConnectionError("Redis applied claim, reply lost")
            return result

        async def finish_many(self, claims, ttl):
            await super().finish_many(claims, ttl)
            if not self.finishes_lost:
                self.finishes_lost = True
                for claim in claims:
                    claim.finished = False  # No acknowledged receipt at the caller.
                raise ConnectionError("Redis applied completion, reply lost")

    producer = Producer()
    processor = Processor(
        settings, producer, Metrics(), aggregation=AmbiguousStore(settings, redis)
    )
    try:
        await processor.handle(spec, [record()], lambda: True)
        await processor.handle(spec, [record()], lambda: True)
        assert producer.calls == 1
    finally:
        await redis.aclose()


@pytest.mark.parametrize(
    "options,path", [({"aggregator": True}, "id"), ({"aggregate_by": "order.id"}, "order.id")]
)
def test_handler_can_register_aggregator_with_default_or_explicit_key(options, path):
    @kafka_handler(name="join", topics_in=["a", "b"], topics_out=["out"], **options)
    async def join(message, *_):
        return message

    worker = next(w for w in all_workers() if w.name == "join")
    assert worker.kind == "aggregator" and worker.aggregate_by == path


def test_bulk_aggregator_rejected_before_any_work_is_consumed(spec):
    with pytest.raises(ValueError, match="bulk"):
        register_worker(
            replace(
                spec, kind="aggregator", topics_in=["a", "b"], aggregate_by="id", bulk_mode=True
            )
        )


@pytest.mark.parametrize("value", [None, "", {}, [], float("nan"), float("inf")])
def test_invalid_correlation_values_are_rejected(spec, value):
    with pytest.raises(ValueError):
        compute_aggregate_key(replace(spec, aggregate_by="id"), {"id": value})


async def test_missing_default_join_id_goes_to_dlq_instead_of_generated_correlation(settings, spec):
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = AggregationStore(settings, redis)
    producer = Producer()
    worker = replace(spec, kind="aggregator", topics_in=["a", "b"], aggregate_by="id")
    try:
        await Processor(settings, producer, Metrics(), aggregation=store).handle(
            worker, [record(value=b'{"text":"missing id"}', topic="a")], lambda: True
        )
        assert producer.sent[0][0] == settings.error_topic
        assert not await redis.keys("qf:agg:*")
    finally:
        await redis.aclose()


async def test_same_token_recovers_claim_after_lost_reply(settings, spec):
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = AggregationStore(settings, redis)
    spec = replace(spec, topics_in=["a", "b"])
    try:
        await store.claim(spec, "job", "a", {"a": 1})
        first = await store.claim(spec, "job", "b", {"b": 2}, token="stable-owner")
        recovered = await store.claim(spec, "job", "b", {"b": 999}, token="stable-owner")
        assert first.status == recovered.status == "ready"
        assert recovered.payload == {"a": 1, "b": 2}
        assert (await store.claim(spec, "job", "b", {}, token="other-owner")).status == "busy"
    finally:
        await redis.aclose()


async def test_finish_is_retryable_after_reply_loss_but_fences_other_tokens(settings, spec):
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = AggregationStore(settings, redis)
    try:
        claim = await store.claim(spec, "job", "in", {"id": "job"})
        await store.finish(claim, 60)
        await store.finish(claim, 60)
        with pytest.raises(RuntimeError):
            await store.finish(replace(claim, token="wrong-owner", finished=False), 60)
    finally:
        await redis.aclose()
