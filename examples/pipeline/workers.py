"""Small deterministic transformations; replace branches with real async clients."""

from framework.decorators import kafka_aggregator, kafka_handler, retry_to_dlq
from framework.tracing import get_tracer

from . import topics


@retry_to_dlq(max_attempts=3, dlq_topic=topics.DLQ)
@kafka_handler(
    name=f"{topics.PREFIX}.normalize",
    topics_in=[topics.INPUT],
    topics_out=[topics.STATS_INPUT, topics.PREVIEW_INPUT],
    max_workers=8,
)
async def normalize(message, consumer_name, metadatas):
    text = message["text"].strip()
    if not text:
        raise ValueError("Text must contain a non-whitespace character")
    return {**message, "text": text}


@kafka_handler(
    name=f"{topics.PREFIX}.statistics",
    topics_in=[topics.STATS_INPUT],
    topics_out=[topics.STATS_OUTPUT],
    max_workers=8,
)
async def statistics(message, consumer_name, metadatas):
    with get_tracer().start_as_current_span("demo.statistics"):
        return {**message, "statistics": {"words": len(message["text"].split())}}


@kafka_handler(
    name=f"{topics.PREFIX}.preview",
    topics_in=[topics.PREVIEW_INPUT],
    topics_out=[topics.PREVIEW_OUTPUT],
    max_workers=8,
)
async def preview(message, consumer_name, metadatas):
    return {**message, "preview": message["text"][:80].upper()}


@kafka_aggregator(
    name=f"{topics.PREFIX}.join",
    topics_in=[topics.STATS_OUTPUT, topics.PREVIEW_OUTPUT],
    topics_out=[topics.OUTPUT],
    aggregate_by="id",
    aggregator_timeout_sec=300,
    max_workers=8,
)
async def join(message, consumer_name, metadatas):
    return {**message, "complete": True}
