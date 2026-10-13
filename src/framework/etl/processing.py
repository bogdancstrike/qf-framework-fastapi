"""ACK-gated processing and separately budgeted handler/transport retries."""

from __future__ import annotations

import asyncio
import contextvars
import copy
import functools
import hashlib
import inspect
import json
import random
import uuid
from dataclasses import dataclass, field
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import Link, SpanKind

from ..commons.logger import logger
from ..decorators.kafka_workers import _get_by_path, compute_aggregate_key
from ..streams.delivery import AckPublisher, Publication, is_permanent_send_error
from ..streams.encoding import compact_json, python_json
from ..tracing import extract_context_from_headers, get_tracer, inject_trace_headers
from .backoff import ExponentialBackoff
from .dead_letter import RAW, DeadLetter, DiagnosticEnvelope
from .observer import JOB_OUTCOMES, notify
from .outcomes import Outcome, is_outcome_result
from .records import context_for

#: Default wait schedule of ``transient`` exceptions: about 1, 2, 4 … 60 s.
DEFAULT_TRANSIENT_BACKOFF = ExponentialBackoff()


class DeliveryError(RuntimeError):
    pass


class StaleAssignment(RuntimeError):
    pass


@dataclass
class Item:
    """One logical message: a decoded object (or array element) and its source record.

    ``original`` is what a dead letter carries by default: ``RAW`` (the record's
    bytes, unmodified) for an object value, the element itself for an array element.
    """

    record: Any
    element_index: int
    payload: Any
    original: Any = RAW
    outcome: str | None = field(default=None)


class Processor:
    """Execute business work with separate application and transport retry budgets.

    True/None report successful processing (including filtering/storing a part),
    False reports an application failure safely routed to DLQ. DeliveryError or
    StaleAssignment leave source work replayable. External business effects are
    outside the Kafka/Redis completion boundary and must handle replay.

    Handlers returning :class:`~framework.etl.outcomes.Outcome` objects get
    per-message settlement (outputs, skips, dead letters and retries per item);
    other results keep the whole-job contract.
    """

    def __init__(
        self,
        settings,
        producer,
        metrics,
        executors=None,
        aggregation=None,
        *,
        producers=None,
        observer=None,
    ):
        self.settings, self.producer, self.metrics = settings, producer, metrics
        self.observer = observer
        self.executors = executors or {}
        self.aggregation = aggregation
        self.publisher = AckPublisher(
            producer, metrics, self.transport, settings.output_ack_timeout_sec
        )
        # Producers of other clusters (workers with their own `bootstrap_servers`).
        self.producers = dict(producers or {})
        self._publishers: dict[str, AckPublisher] = {}
        self.deduplicator = None
        if settings.idempotency_enabled:
            from ..redis.idempotency import SourceDeduplicator

            self.deduplicator = SourceDeduplicator(aggregation, self.transport, metrics)

    # ------------------------------------------------------------- plumbing
    def publisher_for(self, spec) -> AckPublisher:
        """The publisher of the worker's own cluster (the default cluster otherwise)."""
        bootstrap = spec.bootstrap_servers
        if bootstrap is None or bootstrap == self.settings.kafka_bootstrap_servers:
            return self.publisher
        if bootstrap not in self.producers:
            raise RuntimeError(f"No producer for bootstrap servers {bootstrap!r}")
        if bootstrap not in self._publishers:
            self._publishers[bootstrap] = AckPublisher(
                self.producers[bootstrap],
                self.metrics,
                self.transport,
                self.settings.output_ack_timeout_sec,
            )
        return self._publishers[bootstrap]

    def group_for(self, spec) -> str:
        """The consumer group that fetched this worker's records."""
        return spec.consumer_group or self.settings.consumer_name

    async def delay(self, attempt, backoff=None):
        if backoff is not None:
            await asyncio.sleep(backoff.delay(attempt))
            return
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

    async def invoke(self, spec, payload, contexts=None):
        """Isolate mutable inputs per attempt and propagate context to sync threads.

        Cancellation stops awaiting a synchronous handler, not its running
        thread. Native coroutine handlers remain the recommended execution path.
        """
        args = (copy.deepcopy(payload), self.group_for(spec), copy.deepcopy(spec.metadatas))
        kwargs = {}
        if spec.with_context:
            kwargs["context"] = list(contexts) if spec.bulk_mode else contexts[0]
        if inspect.iscoroutinefunction(spec.fn):
            return await spec.fn(*args, **kwargs)
        context = contextvars.copy_context()
        call = functools.partial(context.run, spec.fn, *args, **kwargs)
        result = await asyncio.get_running_loop().run_in_executor(
            self.executors.get(spec.name), call
        )
        if inspect.isawaitable(result):
            return await result
        return result

    def count(self, spec, label, amount=1):
        """Count ``amount`` messages with outcome ``label`` (metrics and the job report)."""
        self.metrics.processed.labels(spec.name, label).inc(amount)
        outcomes = JOB_OUTCOMES.get()
        if outcomes is not None:
            outcomes[label] += amount

    @staticmethod
    def check(valid):
        if not valid():
            raise StaleAssignment("Partition assignment or processing lease is no longer owned")

    def contexts(self, spec, items):
        return [
            context_for(i.record, spec.name, self.group_for(spec), i.element_index) for i in items
        ]

    async def send(self, spec, publisher, publications, worker, kind, check, permanent=None):
        """Publish through ``publisher``; a ``kafka.publish`` span when ``trace_kafka_operations``."""
        if not publications or not self.settings.trace_kafka_operations:
            return await publisher.publish(publications, worker, kind, check, permanent=permanent)
        with get_tracer().start_as_current_span("kafka.publish", kind=SpanKind.PRODUCER) as span:
            span.set_attribute("messaging.system", "kafka")
            span.set_attribute(
                "messaging.destination.name", ",".join(sorted({p.topic for p in publications}))
            )
            span.set_attribute("messaging.batch.message_count", len(publications))
            span.set_attribute("qf.publication.kind", kind)
            return await publisher.publish(publications, worker, kind, check, permanent=permanent)

    @staticmethod
    def key_for(spec, value, record):
        """Output key: the ``key_by`` path's value, else the source record key."""
        if spec.key_by is not None and isinstance(value, dict):
            found = _get_by_path(value, spec.key_by)
            if found is not None:
                return str(found).encode()
        return record.key

    @staticmethod
    def encode(spec, value) -> bytes:
        return (spec.encoder or compact_json)(value)

    # ------------------------------------------------------ whole-job outputs
    async def publish(self, spec, result, records, valid, source_payload=None):
        if result is None:
            return
        values = result if isinstance(result, list) else [result]
        batch_context = trace.get_current_span().get_span_context()
        sources = {}
        if len(records) > 1 and batch_context.is_valid and isinstance(source_payload, list):
            for value, record in zip(source_payload, records):
                key = self.key_for(spec, value, record)
                if key is not None:
                    sources[key] = record.headers
        encoded = []
        for value in values:
            if value is None:
                continue
            if not isinstance(value, dict):
                raise TypeError("Workers must return dict, list[dict], or None")
            encoded.append((self.encode(spec, value), self.key_for(spec, value, records[0])))
        if encoded and not spec.topics_out:
            raise TypeError(f"Worker '{spec.name}' has no output topics but returned outputs")

        publications, spans = [], []
        headers = inject_trace_headers()
        try:
            for body, key in encoded:
                for topic in spec.topics_out:
                    if key in sources:
                        outgoing_headers = self._source_headers(
                            topic, sources[key], batch_context, spans
                        )
                    else:
                        outgoing_headers = headers
                    publications.append(Publication(topic, body, key, outgoing_headers))
            await self.send(
                spec,
                self.publisher_for(spec),
                publications,
                spec.name,
                "output",
                lambda: self.check(valid),
            )
        finally:
            for span in spans:
                span.end()

    @staticmethod
    def _source_headers(topic, source_headers, batch_context, spans):
        """Trace headers continuing one source record's trace inside a batch job."""
        span = get_tracer().start_span(
            f"kafka.publish {topic}",
            context=extract_context_from_headers(source_headers),
            kind=SpanKind.PRODUCER,
            links=[Link(batch_context)],
        )
        spans.append(span)
        with trace.use_span(span, end_on_exit=False):
            return inject_trace_headers()

    # ------------------------------------------------------------ dead letters
    @staticmethod
    def dlq_headers(record, base=None):
        # A bulk failure must preserve each source trace, including malformed records.
        existing = record.headers if base is None else base
        context = extract_context_from_headers(record.headers)
        source = trace.get_current_span(context).get_span_context()
        current = trace.get_current_span().get_span_context()
        if source.is_valid and source.trace_id != current.trace_id:
            with trace.use_span(trace.NonRecordingSpan(source)):
                return inject_trace_headers(existing)
        return inject_trace_headers(existing)

    async def dlq(self, spec, records, error, attempts, valid, *, aggregate=None):
        """Dead-letter whole records (the 2.0 entry point, kept for joins and decoding)."""
        letters = [
            DeadLetter(
                worker=spec.name,
                consumer_group=self.group_for(spec),
                record=record,
                stage="handler" if attempts else "decode",
                attempts=attempts,
                error=error,
                value=RAW,
                aggregate=aggregate,
            )
            for record in records
        ]
        await self.dead_letters(spec, letters, valid)

    async def dead_letters(self, spec, letters, valid):
        """Publish ``letters`` with the worker's format; oversized ones become references."""
        if not letters:
            return
        policy = spec.retry_to_dlq
        topic = policy.dlq_topic if policy else self.settings.error_topic
        dlq_format = (policy.dlq_format if policy else None) or DiagnosticEnvelope()
        publications = []
        for letter in letters:
            self.check(valid)
            built = dlq_format.build(letter)
            publications.append(
                Publication(
                    topic, built.value, built.key, self.dlq_headers(letter.record, built.headers)
                )
            )
        publisher = self.publisher_for(spec)
        check = lambda: self.check(valid)  # noqa: E731
        await self.send(
            spec,
            publisher,
            publications,
            spec.name,
            "dlq",
            check,
            permanent=is_permanent_send_error,
        )
        refused = [p for p in publications if p.failed is not None]
        if refused:
            # Never commit a record without its dead letter: send a reference instead.
            references = [
                Publication(
                    p.topic,
                    python_json(
                        {
                            "_truncated": True,
                            "size": len(p.value),
                            "sha256": hashlib.sha256(p.value).hexdigest(),
                        }
                    ),
                    p.key,
                    p.headers,
                )
                for p in refused
            ]
            self.metrics.dlq_truncated.labels(spec.name).inc(len(references))
            logger.error(
                "Dead letter refused by the broker; published a reference",
                extra={"event": {"worker": spec.name, "topic": topic, "count": len(references)}},
            )
            await self.send(spec, publisher, references, spec.name, "dlq", check)
        self.count(spec, "dlq", len(letters))
        logger.warning(
            "Input routed to DLQ",
            extra={
                "event": {
                    "worker": spec.name,
                    "topic": topic,
                    "count": len(letters),
                    "attempts": max(letter.attempts for letter in letters),
                    "error_type": type(letters[0].error).__name__
                    if letters[0].error is not None
                    else letters[0].stage,
                }
            },
        )

    # ------------------------------------------------------------- decoding
    async def decode(self, spec, records, valid):
        """Decode records into items; undecodable records are dead-lettered at once."""
        if spec.raw:  # undecoded bytes: nothing to reject
            return [
                Item(record, 0, record.value if record.value is not None else b"")
                for record in records
            ]
        items, rejected = [], []
        for record in records:
            try:
                value = json.loads(record.value)
            except (ValueError, TypeError, UnicodeError) as exc:
                if spec.split_arrays:
                    rejected.append(self._decode_letter(spec, record, f"invalid_json: {exc}", exc))
                else:
                    await self.dlq(spec, [record], exc, 0, valid)
                continue
            if isinstance(value, dict):
                elements = [(0, value, RAW)]
            elif spec.split_arrays and isinstance(value, list):
                if not value:
                    self.count(spec, "skipped")
                    continue
                elements = [
                    (i, v, copy.deepcopy(v)) for i, v in enumerate(value) if isinstance(v, dict)
                ]
                dropped = len(value) - len(elements)
                if not elements:
                    rejected.append(self._decode_letter(spec, record, "not_an_object"))
                    continue
                if dropped:
                    self.metrics.decode_dropped.labels(spec.name, "non_object_element").inc(dropped)
                    logger.warning(
                        "Dropped non-object array elements",
                        extra={
                            "event": {"worker": spec.name, "count": dropped, "topic": record.topic}
                        },
                    )
            else:
                if spec.split_arrays:
                    rejected.append(self._decode_letter(spec, record, "not_an_object"))
                else:
                    await self.dlq(
                        spec, [record], ValueError("Kafka payload must be a JSON object"), 0, valid
                    )
                continue
            for index, element, original in elements:
                if (
                    spec.inject_id
                    and (element.get("id") is None or element["id"] == "")
                    and not (spec.kind == "aggregator" and spec.aggregate_by == "id")
                ):
                    suffix = f":{index}" if index else ""
                    element["id"] = f"{record.topic}:{record.partition}:{record.offset}{suffix}"
                items.append(Item(record, index, element, original))
        await self.dead_letters(spec, rejected, valid)
        return items

    def _decode_letter(self, spec, record, reason, error=None):
        return DeadLetter(
            worker=spec.name,
            consumer_group=self.group_for(spec),
            record=record,
            stage="decode",
            attempts=0,
            error=error,
            reason=reason,
            value=RAW,
        )

    # -------------------------------------------------------------- handling
    async def handle(self, spec, records, valid):
        """Process records under assignment and optional source-ledger ownership."""
        if self.deduplicator and spec.kind != "aggregator":
            async with self.deduplicator.filter(spec, records, valid) as selection:
                selected, owned = selection
                if len(selected) < len(records):
                    self.count(spec, "duplicate", len(records) - len(selected))
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
                items = await self.decode(spec, records, valid)
                if not items:
                    return
                if spec.kind == "aggregator":
                    return await self.aggregate(spec, items[0].record, items[0].payload, valid)
                if spec.bulk_mode:
                    return await self.execute(spec, items, valid)
                outcome = True
                for item in items:  # one logical message per call, in array order
                    if await self.execute(spec, [item], valid) is False:
                        outcome = False
                return outcome

    async def execute(self, spec, items, valid, *, aggregate=None, payload=None):
        """Run the handler for ``items`` until every one has a terminal result.

        Ordinary failures use the worker's attempt budget and end in dead letters;
        ``transient`` exceptions are retried with backoff without limit.
        """
        policy = spec.retry_to_dlq
        attempts = policy.max_attempts if policy else self.settings.retry_attempts
        backoff = policy.backoff if policy else None
        transient = policy.transient if policy else ()
        pending = list(items)
        attempt, waits = 0, 0
        while True:
            self.check(valid)
            if payload is not None:
                call_payload = payload
            elif spec.bulk_mode:
                call_payload = [item.payload for item in pending]
            else:
                call_payload = pending[0].payload
            try:
                result = await self.invoke(spec, call_payload, self.contexts(spec, pending))
                if is_outcome_result(result):
                    outcomes = [result] if isinstance(result, Outcome) else list(result)
                    if len(outcomes) != len(pending):
                        await self.dead_letters(
                            spec,
                            [
                                self._letter(spec, i, "outcome_count_mismatch", attempt + 1)
                                for i in pending
                            ],
                            valid,
                        )
                        logger.error(
                            "Handler returned the wrong number of outcomes",
                            extra={
                                "event": {
                                    "worker": spec.name,
                                    "messages": len(pending),
                                    "outcomes": len(outcomes),
                                }
                            },
                        )
                        return False
                    retry = await self.settle(spec, pending, outcomes, valid, attempt + 1)
                else:
                    await self.publish(
                        spec,
                        result,
                        [i.record for i in pending],
                        valid,
                        source_payload=call_payload,
                    )
                    self.count(spec, "success", len(pending))
                    if waits:
                        notify(self.observer, "on_recovered", spec.name, waits)
                    return True
            except (DeliveryError, StaleAssignment, asyncio.CancelledError):
                raise
            except Exception as exc:
                if transient and isinstance(exc, transient):
                    self.metrics.retries.labels(spec.name, "transient").inc()
                    logger.warning(
                        "Transient failure; retrying the same messages",
                        extra={
                            "event": {
                                "worker": spec.name,
                                "error_type": type(exc).__name__,
                                "wait": waits,
                            }
                        },
                    )
                    delay = (policy.transient_backoff or DEFAULT_TRANSIENT_BACKOFF).delay(waits)
                    notify(self.observer, "on_transient", spec.name, waits, delay, exc)
                    await asyncio.sleep(delay)
                    waits += 1
                    continue
                self.metrics.failures.labels("handler").inc()
                if attempt + 1 == attempts:
                    letters = [
                        DeadLetter(
                            worker=spec.name,
                            consumer_group=self.group_for(spec),
                            record=item.record,
                            stage="handler",
                            attempts=attempts,
                            error=exc,
                            value=item.original,
                            element_index=item.element_index,
                            aggregate=aggregate,
                        )
                        for item in pending
                    ]
                    await self.dead_letters(spec, letters, valid)
                    return False
                self.metrics.retries.labels(spec.name, "handler").inc()
                await self.delay(attempt, backoff)
                attempt += 1
                continue
            if waits:
                notify(self.observer, "on_recovered", spec.name, waits)
                waits = 0
            if not retry:
                return True
            if attempt + 1 == attempts:
                await self.dead_letters(
                    spec,
                    [
                        self._letter(spec, item, reason or "retry_exhausted", attempts)
                        for item, reason in retry
                    ],
                    valid,
                )
                return False
            self.metrics.retries.labels(spec.name, "handler").inc(len(retry))
            await self.delay(attempt, backoff)
            attempt += 1
            pending = [item for item, _ in retry]

    def _letter(self, spec, item, reason, attempts, *, stage="handler", value=None, headers=None):
        return DeadLetter(
            worker=spec.name,
            consumer_group=self.group_for(spec),
            record=item.record,
            stage=stage,
            attempts=attempts,
            reason=reason,
            value=item.original if value is None else value,
            element_index=item.element_index,
            extra_headers=headers or {},
        )

    async def settle(self, spec, items, outcomes, valid, attempts):
        """Apply per-message outcomes; return the ``(item, reason)`` pairs to retry.

        Every output is encoded before anything is published, so an encoding error
        retries the whole call without duplicating already-published outputs.
        """
        publications, owners, letters, retry = [], [], [], []
        multi = len(items) > 1
        batch_context = trace.get_current_span().get_span_context()
        spans = []
        try:
            for item, outcome in zip(items, outcomes):
                if outcome.kind == "ok":
                    if outcome.outputs and not spec.topics_out:
                        raise TypeError(
                            f"Worker '{spec.name}' has no output topics but returned outputs"
                        )
                    for document in outcome.outputs:
                        body = self.encode(spec, document)
                        key = self.key_for(spec, document, item.record)
                        for topic in spec.topics_out:
                            headers = (
                                self._source_headers(
                                    topic, item.record.headers, batch_context, spans
                                )
                                if multi and batch_context.is_valid and item.record.headers
                                else inject_trace_headers()
                            )
                            publications.append(Publication(topic, body, key, headers))
                            owners.append((item, outcome, document))
                elif outcome.kind == "dead_letter":
                    values = outcome.dlq_values or (None,)
                    for value in values:
                        letters.append(
                            self._letter(
                                spec,
                                item,
                                outcome.reason or "rejected",
                                attempts,
                                stage=outcome.stage,
                                value=value,
                                headers=outcome.dlq_headers,
                            )
                        )
                elif outcome.kind == "retry":
                    retry.append((item, outcome.reason))
            await self.send(
                spec,
                self.publisher_for(spec),
                publications,
                spec.name,
                "output",
                lambda: self.check(valid),
                permanent=is_permanent_send_error,
            )
        finally:
            for span in spans:
                span.end()
        forward_failed = set()
        for publication, (item, outcome, document) in zip(publications, owners):
            if publication.failed is None:
                continue
            forward_failed.add(id(item))
            error = publication.failed
            letters.append(
                DeadLetter(
                    worker=spec.name,
                    consumer_group=self.group_for(spec),
                    record=item.record,
                    stage="forward",
                    attempts=attempts,
                    error=error,
                    value=document,
                    element_index=item.element_index,
                    extra_headers={**outcome.dlq_headers, "target_topic": publication.topic},
                )
            )
        await self.dead_letters(spec, letters, valid)
        for item, outcome in zip(items, outcomes):
            if (
                outcome.kind == "retry"
                or id(item) in forward_failed
                or outcome.kind == "dead_letter"
            ):
                continue
            label = outcome.label or ("success" if outcome.kind == "ok" else "skipped")
            self.count(spec, label)
        return retry

    # ------------------------------------------------------------------ joins
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
            self.count(spec, outcome)
            return
        try:
            async with self.aggregation.heartbeat(claim):

                def owned():
                    return valid() and not claim.lost

                outcome = await self.execute(
                    spec,
                    [Item(record, 0, claim.payload)],
                    owned,
                    aggregate=claim.payload,
                    payload=claim.payload,
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
