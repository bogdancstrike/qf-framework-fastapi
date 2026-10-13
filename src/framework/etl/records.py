"""Source coordinates of one logical message, passed to handlers that ask for them.

A Kafka record usually carries one JSON object. With ``split_arrays=True`` a record
whose value is a JSON array carries one logical message per object element; each
element then has its own :class:`RecordContext` (same coordinates, its own
``element_index``). Handlers opt in with ``@kafka_handler(..., with_context=True)``
and receive it as the keyword argument ``context`` (a list, aligned with the
messages, for bulk handlers).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

#: ``timestamp_type`` values (Kafka): the producer's clock or the broker's append time.
CREATE_TIME, LOG_APPEND_TIME, NO_TIMESTAMP = 0, 1, -1


@dataclass(frozen=True, slots=True)
class RecordContext:
    """Where a logical message came from and when it was written.

    Attributes:
        worker: Name of the worker handling the message.
        consumer_group: Consumer group that fetched the record.
        topic: Source topic.
        partition: Source partition.
        offset: Source offset.
        key: Record key bytes (``None`` when the producer sent none).
        timestamp_ms: Record timestamp in epoch milliseconds, ``-1`` when absent.
        timestamp_type: ``0`` producer CreateTime, ``1`` broker LogAppendTime, ``-1`` none.
        headers: Record headers as ``(name, bytes)`` pairs, in record order.
        element_index: Position inside a JSON array value; ``0`` for an object value.
    """

    worker: str
    consumer_group: str
    topic: str
    partition: int
    offset: int
    key: bytes | None
    timestamp_ms: int
    timestamp_type: int
    headers: tuple[tuple[str, bytes], ...]
    element_index: int = 0

    @property
    def coordinates(self) -> str:
        """``topic:partition:offset`` plus ``:element_index`` for array elements."""
        base = f"{self.topic}:{self.partition}:{self.offset}"
        return f"{base}:{self.element_index}" if self.element_index else base

    def header(self, name: str) -> bytes | None:
        """The last value of header ``name`` (Kafka allows repeats), or ``None``."""
        found = None
        for key, value in self.headers:
            if key == name:
                found = value
        return found


def record_timestamp(record: Any) -> tuple[int, int]:
    """``(timestamp_ms, timestamp_type)`` of an aiokafka record; ``(-1, -1)`` when absent."""
    timestamp = getattr(record, "timestamp", None)
    kind = getattr(record, "timestamp_type", None)
    if timestamp is None or isinstance(timestamp, bool) or timestamp < 0:
        return -1, NO_TIMESTAMP
    return int(timestamp), int(kind) if kind in (CREATE_TIME, LOG_APPEND_TIME) else CREATE_TIME


def context_for(record: Any, worker: str, group: str, element_index: int = 0) -> RecordContext:
    """Build the :class:`RecordContext` of ``record`` (an aiokafka ``ConsumerRecord``)."""
    timestamp_ms, timestamp_type = record_timestamp(record)
    headers = tuple(
        (str(name), value if isinstance(value, bytes) else str(value).encode())
        for name, value in (getattr(record, "headers", None) or ())
    )
    return RecordContext(
        worker=worker,
        consumer_group=group,
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        key=record.key,
        timestamp_ms=timestamp_ms,
        timestamp_type=timestamp_type,
        headers=headers,
        element_index=element_index,
    )
