"""Event-loop owned fetched-offset tracking; RPC failure never discards progress."""

import asyncio
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PartitionProgress:
    offsets: OrderedDict[int, bool] = field(default_factory=OrderedDict)
    committed: int | None = None


class CommitCoordinator:
    def __init__(self, consumer, metrics=None, timeout=10):
        self.consumer, self.metrics, self.timeout = consumer, metrics, timeout
        self.partitions: dict[Any, PartitionProgress] = {}
        self.lock = asyncio.Lock()

    def assign(self, tp):
        progress = PartitionProgress()
        self.partitions[tp] = progress
        return progress

    def register(self, tp, offset):
        progress = self.partitions[tp]
        if progress.offsets and offset <= next(reversed(progress.offsets)):
            raise ValueError("Fetched offsets must increase within an assignment")
        progress.offsets[offset] = False

    def complete(self, tp, offsets, generation):
        if self.partitions.get(tp) is not generation:
            return False
        for offset in offsets:
            if offset in generation.offsets:
                generation.offsets[offset] = True
        return True

    def candidate(self, progress):
        last = None
        for offset, done in progress.offsets.items():
            if not done:
                break
            last = offset + 1
        return last

    async def commit(self, partitions=None):
        async with self.lock:
            snapshots = {
                tp: (p, self.candidate(p))
                for tp, p in self.partitions.items()
                if partitions is None or tp in partitions
            }
            updates = {tp: off for tp, (_, off) in snapshots.items() if off is not None}
            if not updates:
                return True
            try:
                await asyncio.wait_for(self.consumer.commit(updates), self.timeout)
            except Exception:
                if self.metrics:
                    self.metrics.failures.labels("commit").inc()
                return False
            for tp, off in updates.items():
                progress = snapshots[tp][0]
                if self.partitions.get(tp) is not progress:
                    continue
                while progress.offsets and next(iter(progress.offsets)) < off:
                    progress.offsets.popitem(last=False)
                progress.committed = off
            return True
