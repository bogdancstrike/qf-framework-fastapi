import asyncio
from aiokafka import TopicPartition
from framework.etl.coordinator import CommitCoordinator
from conftest import Consumer


async def test_out_of_order_and_numeric_gaps():
    consumer = Consumer()
    c = CommitCoordinator(consumer)
    tp = TopicPartition("in", 0)
    generation = c.assign(tp)
    for offset in [10, 12, 17]:
        c.register(tp, offset)
    c.complete(tp, [12, 17], generation)
    await c.commit()
    assert not consumer.commits
    c.complete(tp, [10], generation)
    await c.commit()
    assert consumer.commits[tp] == 18
    assert not generation.offsets


async def test_commit_failure_retains_retry_candidate():
    consumer = Consumer(commit_failures=1)
    c = CommitCoordinator(consumer)
    tp = consumer.tp
    generation = c.assign(tp)
    c.register(tp, 0)
    c.complete(tp, [0], generation)
    assert not await c.commit()
    assert c.candidate(generation) == 1
    assert await c.commit()
    assert consumer.commits[tp] == 1


async def test_old_assignment_cannot_complete_new_generation():
    c = CommitCoordinator(Consumer())
    tp = TopicPartition("in", 0)
    old = c.assign(tp)
    c.register(tp, 1)
    new = c.assign(tp)
    c.register(tp, 1)
    assert not c.complete(tp, [1], old)
    assert c.candidate(new) is None


async def test_commit_serialized_and_does_not_remove_later_completion():
    consumer = Consumer()
    c = CommitCoordinator(consumer)
    generation = c.assign(consumer.tp)
    c.register(consumer.tp, 0)
    c.complete(consumer.tp, [0], generation)
    await asyncio.gather(c.commit(), c.commit())
    assert len(consumer.commit_calls) == 1
