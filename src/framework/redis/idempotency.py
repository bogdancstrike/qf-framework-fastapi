"""Optional source-record deduplication ledger with renewable Redis claims.

Completes after output ACK. The ACK-to-ledger crash window still permits duplicates.
"""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace

from ..commons.logger import logger


class SourceDeduplicator:
    def __init__(self, store, transport, metrics=None):
        self.store = store
        self.transport = transport
        self.metrics = metrics

    @asynccontextmanager
    async def filter(self, spec, records, valid):
        from ..etl.processing import Processor

        requests, selected = [], []
        seen = set()
        ledger_specs = {}
        for record in records:
            key = f"source:{record.topic}:{record.partition}:{record.offset}"
            if key in seen:
                continue
            seen.add(key)
            if record.topic not in ledger_specs:
                ledger_specs[record.topic] = replace(
                    spec,
                    name="dedup:" + spec.name,
                    topics_in=[record.topic],
                    aggregator_timeout_sec=self.store.settings.idempotency_ttl_sec,
                )
            requests.append(
                self.store.prepare_claim(ledger_specs[record.topic], key, record.topic, {"id": key})
            )
        claims = [request.claim for request in requests]

        def owned():
            return valid() and all(not claim.lost for claim in claims)

        try:
            async with self.store.heartbeat_many(claims):
                pending = list(requests)
                while pending:
                    Processor.check(owned)
                    await self.transport(lambda: self.store.claim_many(pending), spec.name)
                    pending = [request for request in pending if request.claim.status == "busy"]
                    if pending:
                        if self.metrics:
                            self.metrics.idempotency.labels(spec.name, "wait").inc(len(pending))
                        await asyncio.sleep(0.05)
                unique_records = list(
                    {(r.topic, r.partition, r.offset): r for r in records}.values()
                )
                selected = [
                    record
                    for record, claim in zip(unique_records, claims)
                    if claim.status == "ready"
                ]
                if self.metrics:
                    self.metrics.idempotency.labels(spec.name, "claimed").inc(len(selected))
                    self.metrics.idempotency.labels(spec.name, "skipped").inc(
                        len(records) - len(selected)
                    )
                yield selected, owned
                Processor.check(owned)

                async def finish():
                    Processor.check(valid)
                    await self.store.finish_many(
                        [claim for claim in claims if claim.status == "ready"],
                        self.store.settings.idempotency_ttl_sec,
                    )

                await self.transport(finish, spec.name)
        finally:
            try:
                await self.store.release_many(claims)
            except Exception:
                # Token leases expire; cleanup failure must not hide the original error.
                logger.warning("Source ledger claim cleanup failed; leases will expire")
