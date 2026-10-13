"""Consumer-group and offset helpers for operational commands (cut-over, lag, replay).

``KafkaAdmin`` wraps the aiokafka admin client and short-lived consumers for the
operations an ingestion service needs outside its runtime:

- read a group's committed offsets and the topics' end offsets (lag reports);
- find the offsets at a timestamp (replay "since");
- check whether a group has active members;
- commit offsets to a group **without joining it** (manual assignment), for example
  to migrate a legacy group's position to a new per-flow group.

Committing to a group that has active members is refused unless ``force=True``,
because a live member would overwrite (or be confused by) the new offsets.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import OffsetAndMetadata


class GroupActiveError(RuntimeError):
    """The consumer group has active members (the operation would race them)."""


class KafkaAdmin:
    """Async context manager over one cluster: ``async with KafkaAdmin(servers) as admin``."""

    def __init__(self, bootstrap_servers: str, **security):
        self.bootstrap_servers, self.security = bootstrap_servers, security
        self.client: AIOKafkaAdminClient | None = None
        self.reader: AIOKafkaConsumer | None = None

    async def __aenter__(self) -> KafkaAdmin:
        self.client = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers, **self.security)
        await self.client.start()
        # One group-less consumer for offset lookups (never joins or commits).
        self.reader = self._consumer()
        try:
            await self.reader.start()
        except BaseException:
            await self.client.close()
            raise
        return self

    async def __aexit__(self, *exc) -> None:
        if self.reader is not None:
            await self.reader.stop()
            self.reader = None
        if self.client is not None:
            await self.client.close()
            self.client = None

    def _consumer(self, group: str | None = None) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=group,
            enable_auto_commit=False,
            **self.security,
        )

    def _ready(self) -> tuple[AIOKafkaAdminClient, AIOKafkaConsumer]:
        if self.client is None or self.reader is None:
            raise RuntimeError("Use KafkaAdmin as an async context manager")
        return self.client, self.reader

    async def partitions(self, topics: Iterable[str]) -> list[TopicPartition]:
        """Every partition of ``topics`` (unknown topics have none)."""
        client, _ = self._ready()
        topics = list(topics)
        if not topics:
            return []
        result = []
        for description in await client.describe_topics(topics):
            if description.get("error_code"):
                continue
            for item in description.get("partitions", []):
                result.append(TopicPartition(description["topic"], item["partition"]))
        return sorted(result)

    async def committed_offsets(
        self, group: str, topics: Iterable[str]
    ) -> dict[TopicPartition, int | None]:
        """``group``'s committed offset per partition of ``topics`` (``None``: never committed)."""
        client, _ = self._ready()
        partitions = await self.partitions(topics)
        offsets = await client.list_consumer_group_offsets(group, partitions=partitions)
        return {
            tp: (offsets[tp].offset if tp in offsets and offsets[tp].offset >= 0 else None)
            for tp in partitions
        }

    async def end_offsets(self, topics: Iterable[str]) -> dict[TopicPartition, int]:
        """The next offset to be written, per partition of ``topics``."""
        _, reader = self._ready()
        partitions = await self.partitions(topics)
        return dict(await reader.end_offsets(partitions)) if partitions else {}

    async def lag(self, group: str, topics: Iterable[str]) -> dict[TopicPartition, int | None]:
        """End offset minus committed offset (``None`` when the group never committed)."""
        topics = list(topics)
        committed = await self.committed_offsets(group, topics)
        ends = await self.end_offsets(topics)
        return {
            tp: (None if committed[tp] is None else max(0, ends[tp] - committed[tp]))
            for tp in committed
        }

    async def offsets_for_time(
        self, topics: Iterable[str], timestamp_ms: int
    ) -> dict[TopicPartition, int]:
        """First offset at or after ``timestamp_ms`` per partition (the end offset when none)."""
        _, reader = self._ready()
        partitions = await self.partitions(topics)
        if not partitions:
            return {}
        found = await reader.offsets_for_times({tp: timestamp_ms for tp in partitions})
        ends = await reader.end_offsets(partitions)
        return {
            tp: (found[tp].offset if found.get(tp) is not None else ends[tp]) for tp in partitions
        }

    async def group_members(self, group: str) -> int:
        """Number of active members of ``group`` (0 for an empty or unknown group)."""
        client, _ = self._ready()
        descriptions = await client.describe_consumer_groups([group])
        total = 0
        for description in descriptions:
            for item in getattr(description, "groups", None) or []:
                # Response v0+: (error_code, group_id, state, protocol_type, protocol, members, …)
                members = item[5] if isinstance(item, (tuple, list)) and len(item) > 5 else []
                total += len(members)
        return total

    async def commit_offsets(
        self,
        group: str,
        offsets: Mapping[TopicPartition, int],
        *,
        metadata: str = "",
        force: bool = False,
    ) -> None:
        """Commit ``offsets`` to ``group`` with manual assignment (the group is never joined).

        Raises:
            GroupActiveError: ``group`` has active members and ``force`` is false.
        """
        if not offsets:
            return
        if not force and await self.group_members(group):
            raise GroupActiveError(f"Consumer group {group!r} has active members")
        consumer = self._consumer(group)
        await consumer.start()
        try:
            consumer.assign(list(offsets))
            await consumer.commit(
                {tp: OffsetAndMetadata(offset, metadata) for tp, offset in offsets.items()}
            )
        finally:
            await consumer.stop()
