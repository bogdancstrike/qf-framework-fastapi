"""Dead-letter formats: how a failed message becomes a Kafka record (Strategy).

The runtime decides *when* a message is dead-lettered and *which* topic receives it;
a :class:`DeadLetterFormat` decides the record's value, key and headers. Pick one per
worker with ``@retry_to_dlq(..., dlq_format=...)``.

* :class:`DiagnosticEnvelope` (default, unchanged from 2.0): a JSON envelope with
  source coordinates, worker, attempts, the error *class* and the raw value in base64.
  The exception text is never included, so secrets in error messages stay out of Kafka.
* :class:`HeaderDeadLetter`: the replayable value itself (the unmodified source bytes,
  or the values a handler chose), with the context in headers: ``reason``,
  ``consumer``, ``failed_at``, ``stage``, the worker, ``source_topic``,
  ``source_partition``, ``source_offset``, ``source_timestamp_ms`` and the caller's
  extra headers. A replay tool can republish the value unchanged to ``source_topic``.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from ..streams.encoding import Encoder, python_json


@dataclass(frozen=True)
class DeadLetter:
    """Everything known about one dead letter, handed to the format.

    Attributes:
        worker: Worker name.
        consumer_group: Consumer group of the source record.
        record: The source aiokafka record (``topic``, ``partition``, ``offset``,
            ``key``, ``value``, ``headers``, ``timestamp``).
        stage: Failed stage: ``decode``, ``handler``, ``forward`` or an application stage.
        attempts: Handler attempts made (``0`` when the handler was never called).
        error: The exception, when one caused the dead letter.
        reason: Error text chosen by the handler (``Outcome.dead_letter``), if any.
        value: The value to dead-letter; ``RAW`` means the source record's bytes.
        element_index: Position of the message inside a JSON array value.
        extra_headers: Additional headers (e.g. ``target_topic`` for stage ``forward``).
        aggregate: The merged join payload, for aggregator workers.
    """

    worker: str
    consumer_group: str
    record: Any
    stage: str
    attempts: int
    error: BaseException | None = None
    reason: str | None = None
    value: Any = None
    element_index: int = 0
    extra_headers: Mapping[str, str] = field(default_factory=dict)
    aggregate: Any = None

    @property
    def raw(self) -> bool:
        """``True`` when the source record's bytes are the dead-letter value."""
        return self.value is RAW

    def reason_text(self) -> str:
        """The handler's reason, else ``<ExceptionClass>: <message>``, else ``unknown``."""
        if self.reason:
            return self.reason
        if self.error is not None:
            message = str(self.error)
            return (
                f"{type(self.error).__name__}: {message}" if message else type(self.error).__name__
            )
        return "unknown"


class _Raw:
    """Marker: dead-letter the source record's value bytes unchanged."""

    def __repr__(self) -> str:
        return "RAW"


RAW: Any = _Raw()


@dataclass(frozen=True)
class DeadLetterRecord:
    """One record to publish on the dead-letter topic."""

    value: bytes
    key: bytes | None
    headers: list[tuple[str, bytes]]


class DeadLetterFormat(Protocol):
    """Strategy: build the dead-letter record of one failed message."""

    def build(self, letter: DeadLetter) -> DeadLetterRecord:
        """The record (value, key, headers) to publish; trace headers are added afterwards."""
        ...


@dataclass(frozen=True)
class DiagnosticEnvelope:
    """The 2.0 diagnostic JSON envelope (default; never carries exception text)."""

    def build(self, letter: DeadLetter) -> DeadLetterRecord:
        """A JSON envelope with coordinates, worker, attempts, error class and raw value."""
        record = letter.record
        envelope: dict[str, Any] = {
            "id": f"{record.topic}:{record.partition}:{record.offset}",
            "source": {
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
                "key_base64": base64.b64encode(record.key).decode()
                if record.key is not None
                else None,
            },
            "worker": letter.worker,
            "attempts": letter.attempts,
            "error": {"type": type(letter.error).__name__ if letter.error else "Rejected"},
            "raw_base64": base64.b64encode(record.value or b"").decode(),
        }
        if letter.reason:  # outcome-mode and split-array letters name their stage
            envelope["stage"] = letter.stage
        if letter.element_index:
            envelope["element_index"] = letter.element_index
        if letter.aggregate is not None:
            envelope["aggregate"] = letter.aggregate
        return DeadLetterRecord(
            json.dumps(envelope).encode(), record.key, list(record.headers or [])
        )


@dataclass(frozen=True)
class HeaderDeadLetter:
    """Replayable value plus context headers (the QImporter-compatible format).

    Attributes:
        worker_header: Header carrying the worker name (``flow`` for flow-based services).
        reason_limit: The ``reason`` header keeps at most this many characters.
        max_value_bytes: A value larger than this is replaced by the reference
            ``{"_truncated": true, "size": <bytes>, "sha256": "<hex>"}`` so the dead
            letter itself is never refused by the broker. ``None`` keeps every value.
        encoder: Encoder of non-bytes values (``python_json`` by default).
        clock: Returns "now" (tests inject a fixed clock).
    """

    worker_header: str = "worker"
    reason_limit: int = 2000
    max_value_bytes: int | None = None
    encoder: Encoder = python_json
    clock: Any = None

    def value_bytes(self, letter: DeadLetter) -> bytes:
        """The dead-letter value: source bytes, a bytes value, or the encoded value."""
        if letter.raw:
            return letter.record.value or b""
        if isinstance(letter.value, (bytes, bytearray)):
            return bytes(letter.value)
        return self.encoder(letter.value)

    def build(self, letter: DeadLetter) -> DeadLetterRecord:
        """The value (or its truncation reference) with the context headers."""
        record = letter.record
        value = self.value_bytes(letter)
        if self.max_value_bytes is not None and len(value) > self.max_value_bytes:
            value = python_json(
                {
                    "_truncated": True,
                    "size": len(value),
                    "sha256": hashlib.sha256(value).hexdigest(),
                }
            )
        now = self.clock() if self.clock else datetime.now(UTC)
        timestamp = getattr(record, "timestamp", None)
        headers: dict[str, str] = {
            "reason": letter.reason_text()[: self.reason_limit],
            "consumer": letter.consumer_group,
            "failed_at": now.isoformat(),
            "stage": letter.stage,
            self.worker_header: letter.worker,
            "source_topic": str(record.topic),
            "source_partition": str(record.partition),
            "source_offset": str(record.offset),
            "source_timestamp_ms": str(timestamp if isinstance(timestamp, int) else -1),
        }
        if letter.element_index:
            headers["source_element_index"] = str(letter.element_index)
        headers.update({str(k): str(v) for k, v in letter.extra_headers.items()})
        return DeadLetterRecord(value, record.key, [(k, v.encode()) for k, v in headers.items()])
