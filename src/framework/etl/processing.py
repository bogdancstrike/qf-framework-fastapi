"""ACK-gated processing and separately budgeted handler/transport retries."""

import asyncio
import base64
import contextvars
import copy
import functools
import inspect
import json
import random

from opentelemetry import trace
from opentelemetry.trace import Link, SpanKind

from ..commons.logger import logger
from ..decorators.kafka_workers import compute_aggregate_key
from ..tracing import get_tracer, inject_trace_headers, extract_context_from_headers


class DeliveryError(RuntimeError):
    pass


class StaleAssignment(RuntimeError):
    pass


class Processor:
    def __init__(self, settings, producer, metrics, executors=None, aggregation=None):
        self.settings, self.producer, self.metrics = settings, producer, metrics
        self.executors = executors or {}
        self.aggregation = aggregation
        self.deduplicator = None
        if settings.idempotency_enabled:
            from ..redis.idempotency import SourceDeduplicator

            self.deduplicator = SourceDeduplicator(aggregation, self.transport)

    async def delay(self, attempt):
        await asyncio.sleep(
            random.uniform(
                0,
                min(
                    self.settings.retry_max_sec,
                    self.settings.retry_base_sec * 2 ** min(attempt, 30),
                ),
            )
        )

    async def transport(self, operation, worker):
        for attempt in range(self.settings.transport_attempts):
            try:
                return await operation()
            except (StaleAssignment, asyncio.CancelledError):
                raise
            except Exception as exc:
                self.metrics.failures.labels("transport").inc()
                if attempt + 1 == self.settings.transport_attempts:
                    raise DeliveryError("Transport retry budget exhausted") from exc
                self.metrics.retries.labels(worker, "transport").inc()
                await self.delay(attempt)

    async def invoke(self, spec, payload):
        args = (copy.deepcopy(payload), self.settings.consumer_name, copy.deepcopy(spec.metadatas))
        if inspect.iscoroutinefunction(spec.fn):
            return await spec.fn(*args)
        context = contextvars.copy_context()
        call = functools.partial(context.run, spec.fn, *args)
        result = await asyncio.get_running_loop().run_in_executor(
            self.executors.get(spec.name), call
        )
        if inspect.isawaitable(result):
            return await result
        return result

    @staticmethod
    def check(valid):
        if not valid():
            raise StaleAssignment("Partition assignment was revoked")

    async def publish(self, spec, result, records, valid, source_payload=None):
        if result is None:
            return
        values = result if isinstance(result, list) else [result]
        batch_context = trace.get_current_span().get_span_context()
        sources = {}
        if len(records) > 1 and batch_context.is_valid and isinstance(source_payload, list):
            sources = {
                str(value["id"]).encode(): record.headers
                for value, record in zip(source_payload, records)
            }
        encoded = []
        for value in values:
            if value is None:
                continue
            if not isinstance(value, dict):
                raise TypeError("Workers must return dict, list[dict], or None")
            key = str(value["id"]).encode() if value.get("id") is not None else records[0].key
            encoded.append(
                (json.dumps(value, separators=(",", ":"), allow_nan=False).encode(), key)
            )

        async def send_all():
            self.check(valid)
            futures = []
            spans = []
            try:
                for body, key in encoded:
                    for topic in spec.topics_out:
                        self.check(valid)
                        if key in sources:
                            span = get_tracer().start_span(
                                f"kafka.publish {topic}",
                                context=extract_context_from_headers(sources[key]),
                                kind=SpanKind.PRODUCER,
                                links=[Link(batch_context)],
                            )
                            spans.append(span)
                            with trace.use_span(span, end_on_exit=False):
                                futures.append(
                                    await self.producer.send(
                                        topic, body, key=key, headers=inject_trace_headers()
                                    )
                                )
                        else:
                            futures.append(
                                await self.producer.send(
                                    topic, body, key=key, headers=inject_trace_headers()
                                )
                            )
                if futures:
                    await asyncio.gather(*futures)
            finally:
                for span in spans:
                    span.end()

        await self.transport(
            lambda: asyncio.wait_for(send_all(), self.settings.output_ack_timeout_sec), spec.name
        )

    async def dlq(self, spec, records, error, attempts, valid, *, aggregate=None):
        topic = spec.retry_to_dlq.dlq_topic if spec.retry_to_dlq else self.settings.error_topic

        async def send_all():
            futures = []
            for record in records:
                self.check(valid)
                envelope = {
                    "id": f"{record.topic}:{record.partition}:{record.offset}",
                    "source": {
                        "topic": record.topic,
                        "partition": record.partition,
                        "offset": record.offset,
                        "key_base64": base64.b64encode(record.key).decode()
                        if record.key is not None
                        else None,
                    },
                    "worker": spec.name,
                    "attempts": attempts,
                    "error": {"type": type(error).__name__},
                    "raw_base64": base64.b64encode(record.value or b"").decode(),
                }
                if aggregate is not None:
                    envelope["aggregate"] = aggregate
                futures.append(
                    await self.producer.send(
                        topic,
                        json.dumps(envelope).encode(),
                        key=record.key,
                        headers=inject_trace_headers(record.headers),
                    )
                )
            await asyncio.gather(*futures)

        await self.transport(
            lambda: asyncio.wait_for(send_all(), self.settings.output_ack_timeout_sec), spec.name
        )
        self.metrics.processed.labels(spec.name, "dlq").inc(len(records))
        logger.warning(
            "Input routed to DLQ",
            extra={
                "event": {
                    "worker": spec.name,
                    "topic": topic,
                    "count": len(records),
                    "attempts": attempts,
                    "error_type": type(error).__name__,
                }
            },
        )

    async def handle(self, spec, records, valid):
        if self.deduplicator and spec.kind != "aggregator":
            async with self.deduplicator.filter(spec, records, valid) as selection:
                selected, owned = selection
                if not selected:
                    self.metrics.processed.labels(spec.name, "duplicate").inc(len(records))
                    return True
                return await self._handle(spec, selected, owned)
        return await self._handle(spec, records, valid)

    async def _handle(self, spec, records, valid):
        with self.metrics.job_duration.labels(spec.name).time():
            with get_tracer().start_as_current_span(
                f"kafka.process {records[0].topic}",
                context=extract_context_from_headers(records[0].headers),
                kind=SpanKind.CONSUMER,
                links=[
                    Link(context)
                    for record in records[1:]
                    if (
                        context := trace.get_current_span(
                            extract_context_from_headers(record.headers)
                        ).get_span_context()
                    ).is_valid
                ],
            ) as span:
                span.set_attribute("worker.name", spec.name)
                span.set_attribute("messaging.batch.message_count", len(records))
                payloads, accepted = [], []
                for record in records:
                    try:
                        value = json.loads(record.value)
                        if not isinstance(value, dict):
                            raise ValueError("Kafka payload must be a JSON object")
                        if not value.get("id"):
                            value["id"] = f"{record.topic}:{record.partition}:{record.offset}"
                    except (ValueError, TypeError, UnicodeError) as exc:
                        await self.dlq(spec, [record], exc, 0, valid)
                    else:
                        payloads.append(value)
                        accepted.append(record)
                if not accepted:
                    return
                if spec.kind == "aggregator":
                    return await self.aggregate(spec, accepted[0], payloads[0], valid)
                payload = payloads if spec.bulk_mode else payloads[0]
                return await self.execute(spec, accepted, payload, valid)

    async def execute(self, spec, records, payload, valid, *, aggregate=None):
        attempts = (
            spec.retry_to_dlq.max_attempts if spec.retry_to_dlq else self.settings.retry_attempts
        )
        for attempt in range(attempts):
            self.check(valid)
            try:
                result = await self.invoke(spec, payload)
                # Type/serialization errors are application failures; broker errors propagate separately.
                await self.publish(spec, result, records, valid, source_payload=payload)
            except (DeliveryError, StaleAssignment, asyncio.CancelledError):
                raise
            except Exception as exc:
                self.metrics.failures.labels("handler").inc()
                if attempt + 1 == attempts:
                    await self.dlq(spec, records, exc, attempts, valid, aggregate=aggregate)
                    return False
                self.metrics.retries.labels(spec.name, "handler").inc()
                await self.delay(attempt)
            else:
                self.metrics.processed.labels(spec.name, "success").inc(len(records))
                return True

    async def aggregate(self, spec, record, payload, valid):
        try:
            key = compute_aggregate_key(spec, payload)
        except ValueError as exc:
            await self.dlq(spec, [record], exc, 0, valid)
            return False
        claim = await self.transport(
            lambda: self.aggregation.claim(spec, key, record.topic, payload), spec.name
        )
        while claim.status == "busy":
            self.check(valid)
            await asyncio.sleep(0.05)
            claim = await self.transport(
                lambda: self.aggregation.claim(spec, key, record.topic, payload), spec.name
            )
        if claim.status != "ready":
            self.metrics.processed.labels(spec.name, "aggregate_stored").inc()
            return
        try:
            async with self.aggregation.heartbeat(claim):

                def owned():
                    return valid() and not claim.lost

                await self.execute(spec, [record], claim.payload, owned, aggregate=claim.payload)
                self.check(owned)
                await self.transport(
                    lambda: self.aggregation.finish(claim, spec.aggregator_timeout_sec), spec.name
                )
        finally:
            await self.aggregation.release(claim)
