"""Track individual producer receipts across bounded transport attempts.

Timeouts preserve pending receipts: cancelling an aiokafka receipt does not retract
an accepted send. Confirmed successful outputs must never be enqueued again by
this publisher. Process death and lost/failed receipts still permit replay.
"""

import asyncio
import time
from dataclasses import dataclass


@dataclass
class Publication:
    topic: str
    value: bytes
    key: bytes | None
    headers: list
    receipt: asyncio.Future | None = None
    acknowledged: bool = False
    started: float = 0


class AckPublisher:
    """ACK receipt state machine; transport supplies the bounded retry policy."""

    def __init__(self, producer, metrics, transport, timeout):
        self.producer, self.metrics = producer, metrics
        self.transport, self.timeout = transport, timeout

    async def publish(self, publications, worker, kind, check):
        """Wait for every receipt without resending known successes.

        Resolve metric children once per job. Bulk ACK callbacks otherwise each
        acquire the Prometheus label-cache lock twice for identical labels.
        """
        if not publications:
            return
        ack_count = self.metrics.output_acks.labels(worker, kind)
        ack_duration = self.metrics.output_ack_duration.labels(worker, kind)

        def observe(publication, receipt):
            # Always retrieve errors, including receipts finishing after cancellation.
            if receipt.cancelled() or receipt.exception() is not None:
                return
            if not publication.acknowledged:
                publication.acknowledged = True
                ack_count.inc()
                ack_duration.observe(max(0, time.perf_counter() - publication.started))

        async def attempt():
            check()
            for publication in publications:
                check()
                if publication.acknowledged:
                    continue
                receipt = publication.receipt
                if receipt is not None and receipt.done():
                    if receipt.cancelled():
                        raise RuntimeError("Kafka receipt cancelled; delivery is uncertain")
                    if receipt.exception() is not None:
                        publication.receipt = None
                    else:
                        observe(publication, receipt)
                        continue
                if publication.receipt is None:
                    publication.started = time.perf_counter()
                    receipt = publication.receipt = await self.producer.send(
                        publication.topic,
                        publication.value,
                        key=publication.key,
                        headers=publication.headers,
                    )
                    receipt.add_done_callback(lambda future, p=publication: observe(p, future))
            receipts = [p.receipt for p in publications if not p.acknowledged]
            if len(receipts) == 1:
                await asyncio.shield(receipts[0])
            elif receipts:
                # Unlike gather under wait_for, wait never cancels producer receipts.
                await asyncio.wait(receipts)
            for publication in publications:
                if publication.receipt is not None:
                    receipt = publication.receipt
                    if receipt.cancelled():
                        raise RuntimeError("Kafka receipt cancelled; delivery is uncertain")
                    if receipt.exception() is not None:
                        raise receipt.exception()
                    observe(publication, receipt)
            check()

        await self.transport(lambda: asyncio.wait_for(attempt(), self.timeout), worker)
