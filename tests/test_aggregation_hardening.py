from dataclasses import replace

import fakeredis.aioredis
import pytest

from framework.decorators import kafka_handler
from framework.decorators.kafka_workers import all_workers, compute_aggregate_key, register_worker
from framework.etl.processing import Processor
from framework.observability import Metrics
from framework.redis.aggregation import AggregationStore
from conftest import Producer, record


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
