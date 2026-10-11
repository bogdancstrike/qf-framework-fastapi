"""Optional source-record deduplication ledger with renewable Redis claims.

Completes after output ACK. The ACK-to-ledger crash window still permits duplicates.
"""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace


class SourceDeduplicator:
    def __init__(self, store, transport):
        self.store = store
        self.transport = transport

    @asynccontextmanager
    async def filter(self, spec, records, valid):
        from ..etl.processing import Processor

        claims, selected = [], []
        try:
            async with AsyncExitStack() as stack:
                for record in records:
                    key = f"source:{record.topic}:{record.partition}:{record.offset}"
                    ledger_spec = replace(
                        spec,
                        name="dedup:" + spec.name,
                        topics_in=[record.topic],
                        aggregator_timeout_sec=self.store.settings.idempotency_ttl_sec,
                    )
                    while True:
                        Processor.check(valid)
                        claim = await self.transport(
                            lambda: self.store.claim(ledger_spec, key, record.topic, {"id": key}),
                            spec.name,
                        )
                        if claim.status != "busy":
                            break
                        await asyncio.sleep(0.05)
                    if claim.status == "ready":
                        claims.append(claim)
                        selected.append(record)
                        await stack.enter_async_context(self.store.heartbeat(claim))
                yield selected, lambda: valid() and all(not claim.lost for claim in claims)
                for claim in claims:
                    Processor.check(valid)
                    if claim.lost:
                        raise RuntimeError("Deduplication lease lost")
                    await self.transport(
                        lambda: self.store.finish(claim, self.store.settings.idempotency_ttl_sec),
                        spec.name,
                    )
        finally:
            for claim in claims:
                await self.store.release(claim)
