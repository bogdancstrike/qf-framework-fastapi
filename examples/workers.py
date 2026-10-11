"""Use async external clients in native workers; no requests or time.sleep here."""

from framework.decorators import kafka_handler, kafka_aggregator, retry_to_dlq
from framework.tracing import get_tracer


@retry_to_dlq(max_attempts=3, dlq_topic="text.dlq")
@kafka_handler(
    name="normalize", topics_in=["text.in"], topics_out=["text.normalized"], max_workers=16
)
async def normalize(message, consumer_name, metadatas):
    with get_tracer().start_as_current_span("text.normalize"):
        message["text"] = message["text"].strip()
        return message


@kafka_handler(
    name="bulk",
    topics_in=["bulk.in"],
    topics_out=["bulk.out"],
    max_workers=8,
    bulk_mode=True,
    batch_size=32,
    batch_timeout_ms=25,
)
async def bulk(messages, consumer_name, metadatas):
    return [{**message, "processed": True} for message in messages]


@kafka_aggregator(
    name="merge",
    topics_in=["analysis.ner", "analysis.sentiment"],
    topics_out=["analysis.merged"],
    aggregate_by="id",
    max_workers=8,
)
async def merge(message, consumer_name, metadatas):
    return {**message, "complete": True}
