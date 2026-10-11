"""ACK-gated processing and separately budgeted handler/transport retries."""

import asyncio
import base64
import contextvars
import copy
import functools
import inspect
import json
import random
import uuid

from opentelemetry import trace
from opentelemetry.trace import Link, SpanKind

from ..commons.logger import logger
from ..decorators.kafka_workers import compute_aggregate_key
from ..tracing import get_tracer, inject_trace_headers, extract_context_from_headers
from ..streams.delivery import AckPublisher, Publication


class DeliveryError(RuntimeError):
    pass


class StaleAssignment(RuntimeError):
    pass


class Processor:
    """Execute business work with separate application and transport retry budgets.

    True/None report successful processing (including filtering/storing a part),
    False reports an application failure safely routed to DLQ. DeliveryError or
    StaleAssignment leave source work replayable. External business effects are
    outside the Kafka/Redis completion boundary and must handle replay.
    """

    def __init__(self, settings, producer, metrics, executors=None, aggregation=None):
        self.settings, self.producer, self.metrics = settings, producer, metrics
        self.executors = executors or {}
        self.aggregation = aggregation
        self.publisher = AckPublisher(
            producer, metrics, self.transport, settings.output_ack_timeout_sec
        )
        self.deduplicator = None
        if settings.idempotency_enabled:
            from ..redis.idempotency import SourceDeduplicator

            self.deduplicator = SourceDeduplicator(aggregation, self.transport, metrics)

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
        """Isolate mutable inputs per attempt and propagate context to sync threads.

        Cancellation stops awaiting a synchronous handler, not its running
        thread. Native coroutine handlers remain the recommended execution path.
        """
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
            raise StaleAssignment("Partition assignment or processing lease is no longer owned")

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

        publications, spans = [], []
        headers = inject_trace_headers()
        try:
            for body, key in encoded:
                for topic in spec.topics_out:
                    if key in sources:
                        span = get_tracer().start_span(
                            f"kafka.publish {topic}",
                            context=extract_context_from_headers(sources[key]),
                            kind=SpanKind.PRODUCER,
                            links=[Link(batch_context)],
                        )
                        spans.append(span)
                        with trace.use_span(span, end_on_exit=False):
                            outgoing_headers = inject_trace_headers()
                    else:
                        outgoing_headers = headers
                    publications.append(Publication(topic, body, key, outgoing_headers))
            await self.publisher.publish(
                publications, spec.name, "output", lambda: self.check(valid)
            )
        finally:
            for span in spans:
                span.end()

    @staticmethod
    def dlq_headers(record):
        # A bulk failure must preserve each source trace, including malformed records.
        context = extract_context_from_headers(record.headers)
        source = trace.get_current_span(context).get_span_context()
        current = trace.get_current_span().get_span_context()
        if source.is_valid and source.trace_id != current.trace_id:
            with trace.use_span(trace.NonRecordingSpan(source)):
                return inject_trace_headers(record.headers)
        return inject_trace_headers(record.headers)

    async def dlq(self, spec, records, error, attempts, valid, *, aggregate=None):
        topic = spec.retry_to_dlq.dlq_topic if spec.retry_to_dlq else self.settings.error_topic

        publications = []
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
            publications.append(
                Publication(
                    topic,
                    json.dumps(envelope).encode(),
                    record.key,
                    self.dlq_headers(record),
                )
            )
        await self.publisher.publish(publications, spec.name, "dlq", lambda: self.check(valid))
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
        """Process records under assignment and optional source-ledger ownership."""
        if self.deduplicator and spec.kind != "aggregator":
            async with self.deduplicator.filter(spec, records, valid) as selection:
                selected, owned = selection
                if len(selected) < len(records):
                    self.metrics.processed.labels(spec.name, "duplicate").inc(
                        len(records) - len(selected)
                    )
                if not selected:
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
                    if record.headers
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
                        if (value.get("id") is None or value["id"] == "") and not (
                            spec.kind == "aggregator" and spec.aggregate_by == "id"
                        ):
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
        """Store a part or process a complete join under one renewable owner token.

        Stored parts can be committed after Redis acknowledgement. A complete
        join is finished only after output or terminal DLQ acknowledgement.
        Returning the handler outcome keeps join circuit policies meaningful.
        """
        try:
            key = compute_aggregate_key(spec, payload)
        except ValueError as exc:
            await self.dlq(spec, [record], exc, 0, valid)
            return False
        token = uuid.uuid4().hex
        while True:
            self.check(valid)
            claim = await self.transport(
                lambda: self.aggregation.claim(spec, key, record.topic, payload, token=token),
                spec.name,
            )
            self.metrics.aggregate_parts.labels(spec.name, claim.status).inc()
            if claim.status != "busy":
                break
            await asyncio.sleep(0.05)
        if claim.status != "ready":
            outcome = "aggregate_duplicate" if claim.status == "done" else "aggregate_stored"
            self.metrics.processed.labels(spec.name, outcome).inc()
            return
        try:
            async with self.aggregation.heartbeat(claim):

                def owned():
                    return valid() and not claim.lost

                outcome = await self.execute(
                    spec, [record], claim.payload, owned, aggregate=claim.payload
                )
                self.check(owned)
                await self.transport(
                    lambda: self.aggregation.finish(claim, spec.aggregator_timeout_sec), spec.name
                )
                return outcome
        finally:
            try:
                await self.aggregation.release(claim)
            except Exception:
                logger.warning("Aggregate claim cleanup failed; lease will expire")
