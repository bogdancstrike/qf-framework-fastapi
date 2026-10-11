"""Async-first, bounded Kafka runtime. All consumer state belongs to its event loop."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import ssl
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener

from ..commons.logger import logger
from ..config import FrameworkSettings
from ..decorators.kafka_workers import all_workers
from ..observability import Metrics
from .coordinator import CommitCoordinator
from .policies import Admission
from .processing import Processor, StaleAssignment


@dataclass
class Partition:
    progress: object
    pending: deque = field(default_factory=deque)
    tasks: set = field(default_factory=set)
    revoked: bool = False
    draining: bool = False


class RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, runtime):
        self.runtime = runtime

    async def on_partitions_revoked(self, revoked):
        await self.runtime.revoke(revoked)

    async def on_partitions_assigned(self, assigned):
        for tp in assigned:
            self.runtime.partitions[tp] = Partition(self.runtime.coordinator.assign(tp))


class KafkaRuntime:
    def __init__(
        self,
        settings: FrameworkSettings,
        *,
        metrics=None,
        consumer=None,
        producer=None,
        aggregation=None,
        workers=None,
    ):
        self.settings, self.metrics = settings, metrics or Metrics()
        self.consumer, self.producer, self.aggregation = consumer, producer, aggregation
        self.workers = workers
        self.partitions = {}
        self.active = {}
        self.executors = {}
        self.admission = {}
        self.coordinator = None
        self.processor = None
        self.task = None
        self.ready = False
        self.error = None
        self.closing = False
        self.commit_failures = 0
        self._closed = False
        self.wakeup = asyncio.Event()

    def security(self):
        s = self.settings
        args = {"security_protocol": s.kafka_security_protocol}
        if "SSL" in s.kafka_security_protocol:
            args["ssl_context"] = ssl.create_default_context(cafile=s.kafka_ssl_cafile)
        if s.kafka_security_protocol.startswith("SASL"):
            args.update(
                sasl_mechanism=s.kafka_sasl_mechanism,
                sasl_plain_username=s.kafka_sasl_username,
                sasl_plain_password=s.kafka_sasl_password.get_secret_value(),
            )
        return args

    async def connect(self, client):
        for attempt in range(self.settings.connect_attempts):
            try:
                await client.start()
                return
            except Exception:
                if attempt + 1 == self.settings.connect_attempts:
                    raise
                await asyncio.sleep(
                    min(
                        self.settings.retry_max_sec,
                        self.settings.connect_retry_base_sec * 2**attempt,
                    )
                )

    async def start(self):
        if self.task is not None or self._closed:
            raise RuntimeError("Runtime instances can only start once")
        try:
            async with asyncio.timeout(self.settings.startup_timeout_sec):
                await self._start()
        except BaseException:
            await self.stop()
            raise

    async def _start(self):
        for module in self.settings.worker_modules:
            importlib.import_module(module)
        self.workers = list(all_workers() if self.workers is None else self.workers)
        if not self.workers:
            raise RuntimeError("No Kafka workers registered")
        self.by_topic = {topic: w for w in self.workers for topic in w.topics_in}
        self.active = {w.name: set() for w in self.workers}
        self.admission = {w.name: Admission(w) for w in self.workers}
        self.executors = {
            w.name: ThreadPoolExecutor(w.max_workers, thread_name_prefix=f"qf-{w.name}")
            for w in self.workers
            if not inspect.iscoroutinefunction(w.fn)
        }
        if self.consumer is None:
            self.consumer = AIOKafkaConsumer(
                bootstrap_servers=self.settings.kafka_bootstrap_servers,
                group_id=self.settings.consumer_name,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                isolation_level="read_committed",
                max_poll_interval_ms=self.settings.max_poll_interval_ms,
                fetch_max_wait_ms=self.settings.fetch_max_wait_ms,
                rebalance_timeout_ms=int(self.settings.rebalance_timeout_sec * 1000),
                **self.security(),
            )
        self.coordinator = CommitCoordinator(
            self.consumer, self.metrics, self.settings.commit_timeout_sec
        )
        self.consumer.subscribe(list(self.by_topic), listener=RebalanceListener(self))
        if self.producer is None:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.settings.kafka_bootstrap_servers,
                enable_idempotence=True,
                acks="all",
                linger_ms=self.settings.producer_linger_ms,
                max_batch_size=self.settings.producer_batch_size,
                **self.security(),
            )
        await self.connect(self.producer)
        if self.settings.idempotency_enabled or any(w.kind == "aggregator" for w in self.workers):
            if self.aggregation is None:
                from ..redis.aggregation import AggregationStore

                self.aggregation = AggregationStore(self.settings)
            await self.aggregation.start()
        self.processor = Processor(
            self.settings, self.producer, self.metrics, self.executors, self.aggregation
        )
        await self.connect(self.consumer)
        self.ready = True
        self.metrics.ready.set(1)
        self.task = asyncio.create_task(self.run(), name="qf-kafka-runtime")

    def valid(self, tp, partition):
        return not partition.revoked and self.partitions.get(tp) is partition

    def schedule(self, tp, partition, *, flush=False):
        if partition.revoked or partition.draining or not partition.pending:
            return
        spec = self.by_topic[tp.topic]
        active = self.active[spec.name]
        admission = self.admission[spec.name]
        while partition.pending and len(active) < spec.max_workers:
            oldest = partition.pending[0][1]
            if (
                spec.bulk_mode
                and not flush
                and len(partition.pending) < spec.batch_size
                and (time.monotonic() - oldest) * 1000 < spec.batch_timeout_ms
            ):
                break
            if not admission.allow():
                break
            n = min(len(partition.pending), spec.batch_size) if spec.bulk_mode else 1
            records = [partition.pending.popleft()[0] for _ in range(n)]
            task = asyncio.create_task(
                self.job(spec, tp, partition, records), name=f"qf-{spec.name}"
            )
            active.add(task)
            partition.tasks.add(task)

            def done(future, active=active, partition=partition):
                active.discard(future)
                partition.tasks.discard(future)
                self.wakeup.set()
                if not future.cancelled():
                    error = future.exception()
                    if error is not None and not isinstance(error, StaleAssignment):
                        self.error = error
                        self.ready = False
                        self.metrics.ready.set(0)

            task.add_done_callback(done)

    async def job(self, spec, tp, partition, records):
        self.metrics.inflight.labels(spec.name).inc()
        try:
            ok = await self.processor.handle(spec, records, lambda: self.valid(tp, partition))
            if ok is False:
                self.admission[spec.name].failure()
            else:
                self.admission[spec.name].success()
            self.coordinator.complete(tp, [r.offset for r in records], partition.progress)
        except Exception:
            self.admission[spec.name].failure()
            raise
        finally:
            self.metrics.inflight.labels(spec.name).dec()

    def enqueue(self, tp, records):
        partition = self.partitions.get(tp)
        if partition is None or partition.revoked or partition.draining:
            return
        progress = partition.progress
        for record in records:
            if len(progress.offsets) >= self.settings.pending_max_per_tp:
                self.consumer.pause(tp)
                self.consumer.seek(tp, record.offset)
                break
            self.coordinator.register(tp, record.offset)
            partition.pending.append((record, time.monotonic()))

    def tick(self, *, flush=False):
        # Rotate order to keep hot partitions from monopolizing shared worker slots.
        for tp, partition in list(self.partitions.items()):
            self.schedule(tp, partition, flush=flush)
            spec = self.by_topic[tp.topic]
            count = len(partition.progress.offsets)
            self.metrics.pending.labels(spec.name, tp.topic, str(tp.partition)).set(
                len(partition.pending)
            )
            self.metrics.uncommitted.labels(tp.topic, str(tp.partition)).set(count)
            admission = self.admission[spec.name]
            self.metrics.circuit.labels(spec.name).set(
                bool(admission.open_until or admission.probe)
            )
            if not partition.revoked and tp in self.consumer.assignment():
                if count >= self.settings.pending_max_per_tp:
                    self.consumer.pause(tp)
                else:
                    self.consumer.resume(tp)
                highwater = self.consumer.highwater(tp)
                if highwater is not None and partition.progress.committed is not None:
                    self.metrics.lag.labels(tp.topic, str(tp.partition)).set(
                        max(0, highwater - partition.progress.committed)
                    )
        if self.partitions:
            tp = next(iter(self.partitions))
            self.partitions[tp] = self.partitions.pop(tp)

    async def commit(self, partitions=None):
        ok = await self.coordinator.commit(partitions)
        self.commit_failures = 0 if ok else self.commit_failures + 1
        if self.commit_failures >= self.settings.transport_attempts:
            raise RuntimeError("Commit retry budget exhausted; source offsets remain replayable")

    async def run(self):
        last_commit = time.monotonic()
        poll_task = None
        try:
            while not self.closing:
                if self.error is not None:
                    raise self.error
                self.wakeup.clear()
                self.tick()
                if poll_task is None:
                    poll_task = asyncio.create_task(
                        self.consumer.getmany(
                            timeout_ms=self.settings.poll_timeout_ms,
                            max_records=self.settings.poll_max_records,
                        )
                    )
                wake_task = asyncio.create_task(self.wakeup.wait())
                started = time.monotonic()
                timeout = (
                    0.005
                    if any(p.pending for p in self.partitions.values())
                    else self.settings.commit_tick_sec
                )
                try:
                    done, _ = await asyncio.wait(
                        {poll_task, wake_task}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    wake_task.cancel()
                    await asyncio.gather(wake_task, return_exceptions=True)
                if poll_task in done:
                    records = poll_task.result()
                    poll_task = None
                    for tp, messages in records.items():
                        self.enqueue(tp, messages)
                now = time.monotonic()
                self.metrics.loop_lag.set(max(0, now - started - timeout))
                if now - last_commit >= self.settings.commit_tick_sec:
                    await self.commit()
                    last_commit = time.monotonic()
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.error = exc
            logger.exception("Kafka runtime failed", extra={"event": {"component": "runtime"}})
        finally:
            if poll_task:
                poll_task.cancel()
                await asyncio.gather(poll_task, return_exceptions=True)
            self.ready = False
            self.metrics.ready.set(0)

    async def revoke(self, revoked):
        targets = {tp: self.partitions[tp] for tp in revoked if tp in self.partitions}
        # Stop dispatching new work but let already-running jobs finish while assignment is owned.
        for p in targets.values():
            p.draining = True
            p.pending.clear()
        tasks = {task for p in targets.values() for task in p.tasks}
        if tasks:
            _, unfinished = await asyncio.wait(
                tasks, timeout=self.settings.rebalance_timeout_sec / 2
            )
        else:
            unfinished = set()
        for p in targets.values():
            p.revoked = True
        for task in unfinished:
            task.cancel()
        if unfinished:
            await asyncio.gather(*unfinished, return_exceptions=True)
        if self.coordinator:
            await self.coordinator.commit(set(targets))
            for tp in targets:
                self.coordinator.partitions.pop(tp, None)
                self.partitions.pop(tp, None)
                spec = self.by_topic[tp.topic]
                self.metrics.pending.labels(spec.name, tp.topic, str(tp.partition)).set(0)
                self.metrics.uncommitted.labels(tp.topic, str(tp.partition)).set(0)
                self.metrics.lag.labels(tp.topic, str(tp.partition)).set(0)

    async def stop(self):
        if self._closed:
            return
        self._closed = True
        self.closing = True
        self.ready = False
        self.metrics.ready.set(0)
        if self.task and self.task is not asyncio.current_task():
            try:
                await asyncio.wait_for(
                    asyncio.shield(self.task), self.settings.shutdown_timeout_sec
                )
            except (TimeoutError, asyncio.CancelledError):
                self.task.cancel()
                await asyncio.gather(self.task, return_exceptions=True)
        deadline = time.monotonic() + self.settings.shutdown_timeout_sec
        while self.coordinator and self.error is None and time.monotonic() < deadline:
            self.tick(flush=True)
            if not any(p.pending or p.tasks for p in self.partitions.values()):
                break
            await asyncio.sleep(0.01)
        for p in self.partitions.values():
            p.revoked = True
        tasks = {t for active in self.active.values() for t in active}
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self.coordinator:
            for _ in range(self.settings.transport_attempts):
                if await self.coordinator.commit():
                    break
        for client in (self.consumer, self.producer, self.aggregation):
            if client is None:
                continue
            try:
                await asyncio.wait_for(client.stop(), self.settings.shutdown_timeout_sec)
            except Exception:
                logger.exception("Resource shutdown failed")
        for executor in self.executors.values():
            executor.shutdown(wait=False, cancel_futures=True)


async def start(*, worker_modules, bootstrap_servers, consumer_name, settings=None):
    """Run workers until cancellation. Prefer lifespan-owned KafkaRuntime for HTTP apps."""
    settings = settings or FrameworkSettings(
        worker_modules=list(worker_modules),
        kafka_bootstrap_servers=bootstrap_servers,
        consumer_name=consumer_name,
    )
    runtime = KafkaRuntime(settings)
    await runtime.start()
    try:
        await runtime.task
        if runtime.error:
            raise RuntimeError("Kafka runtime terminated") from runtime.error
    finally:
        await runtime.stop()
