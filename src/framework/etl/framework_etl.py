"""Async-first, bounded Kafka runtime. All consumer state belongs to its event loop."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import ssl
import time
from collections import Counter
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener
from aiokafka.admin import AIOKafkaAdminClient
from opentelemetry.trace import SpanKind

from ..commons.logger import logger
from ..config import FrameworkSettings
from ..decorators.kafka_workers import all_workers, validate_workers
from ..observability import Metrics
from ..tracing import get_tracer
from .coordinator import CommitCoordinator
from .policies import Admission
from .observer import JOB_OUTCOMES, JobReport, notify
from .processing import Processor, StaleAssignment


class MissingTopicsError(RuntimeError):
    """``verify_topics`` found topics that do not exist (they are never auto-created)."""

    def __init__(self, missing):
        self.missing = sorted(missing)
        super().__init__("Missing Kafka topics: " + ", ".join(self.missing))


@dataclass
class Lane:
    """One consumer and its commit coordinator, shared by workers with equal
    bootstrap servers, consumer group and offset reset policy."""

    bootstrap: str
    group: str
    offset_reset: str
    consumer: Any = None
    coordinator: Any = None
    workers: list = field(default_factory=list)
    index: int = 0
    by_topic: dict = field(default_factory=dict)


@dataclass
class Partition:
    progress: object
    pending: deque = field(default_factory=deque)
    tasks: set = field(default_factory=set)
    revoked: bool = False
    draining: bool = False
    lane: Lane | None = None
    tp: Any = None
    spec: Any = None


class RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, runtime, lane=None):
        self.runtime, self.lane = runtime, lane

    async def on_partitions_revoked(self, revoked):
        await self.runtime.revoke(revoked, self.lane)

    async def on_partitions_assigned(self, assigned):
        lane = self.lane or self.runtime.lanes[0]
        for tp in assigned:
            self.runtime.partitions[self.runtime.pkey(lane, tp)] = Partition(
                lane.coordinator.assign(tp), lane=lane, tp=tp, spec=lane.by_topic.get(tp.topic)
            )


class KafkaRuntime:
    """One process's scheduler, transports and contiguous commit coordinator.

    Constructor injection supports test transports and application composition.
    This runtime starts/stops supplied transports too; an AggregationStore with
    an injected Redis client leaves that underlying client application-owned.
    Runtime instances are single-use and all methods run on their owning loop.

    Workers naming their own ``consumer_group``, ``bootstrap_servers`` or
    ``auto_offset_reset`` get their own consumer ("lane"), with its own commits
    and rebalances; workers using the defaults share the first lane, exactly as in
    2.0 (``runtime.consumer``/``runtime.coordinator`` are that lane's). Outputs and
    dead letters use a producer of the worker's own cluster. ``consumer_factory``
    and ``producer_factory`` (``(lane_or_bootstrap) -> client``) inject transports
    for extra lanes and clusters in tests.
    """

    def __init__(
        self,
        settings: FrameworkSettings,
        *,
        metrics=None,
        consumer=None,
        producer=None,
        aggregation=None,
        workers=None,
        consumer_factory=None,
        producer_factory=None,
        observer=None,
    ):
        self.settings, self.metrics = settings, metrics or Metrics()
        self.observer = observer
        self.disabled: list[str] = []
        self.consumer, self.producer, self.aggregation = consumer, producer, aggregation
        self.consumer_factory, self.producer_factory = consumer_factory, producer_factory
        self.workers = workers
        self.lanes: list[Lane] = []
        self.producers: dict[str, Any] = {}
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
        """Validate workers and connect within a bounded startup budget.

        Any startup failure closes partially acquired resources and propagates.
        Successful connection starts a monitored loop; later terminal failures
        set error/ready for HTTP probes and the worker-only runner.
        """
        if self.task is not None or self._closed:
            raise RuntimeError("Runtime instances can only start once")
        try:
            async with asyncio.timeout(self.settings.startup_timeout_sec):
                await self._start()
        except BaseException:
            await self.stop()
            raise

    def lane_key(self, spec):
        return (
            spec.bootstrap_servers or self.settings.kafka_bootstrap_servers,
            spec.consumer_group or self.settings.consumer_name,
            spec.auto_offset_reset,
        )

    def new_consumer(self, lane):
        if self.consumer_factory is not None:
            return self.consumer_factory(lane)
        return AIOKafkaConsumer(
            bootstrap_servers=lane.bootstrap,
            group_id=lane.group,
            enable_auto_commit=False,
            auto_offset_reset=lane.offset_reset,
            isolation_level="read_committed",
            max_poll_interval_ms=self.settings.max_poll_interval_ms,
            fetch_max_wait_ms=self.settings.fetch_max_wait_ms,
            rebalance_timeout_ms=int(self.settings.rebalance_timeout_sec * 1000),
            **self.security(),
        )

    def new_producer(self, bootstrap):
        if self.producer_factory is not None:
            return self.producer_factory(bootstrap)
        return AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            enable_idempotence=True,
            acks="all",
            linger_ms=self.settings.producer_linger_ms,
            max_batch_size=self.settings.producer_batch_size,
            max_request_size=self.settings.producer_max_request_size,
            request_timeout_ms=self.settings.producer_request_timeout_ms,
            **self.security(),
        )

    def select_workers(self, workers):
        """Drop workers declared ``enabled=False`` or named in ``disabled_workers``."""
        disabled = set(self.settings.disabled_workers)
        unknown = disabled - {w.name for w in workers}
        if unknown:
            raise ValueError(
                "disabled_workers names unknown workers: " + ", ".join(sorted(unknown))
            )
        disabled |= {w.name for w in workers if not w.enabled}
        self.disabled = sorted(disabled)
        if disabled:
            logger.info(
                "Kafka workers disabled by configuration",
                extra={"event": {"workers": self.disabled}},
            )
        return [w for w in workers if w.name not in disabled]

    def pkey(self, lane, tp):
        """Partition key: the ``TopicPartition`` in the first lane (2.0), ``(lane, tp)`` else."""
        return tp if lane is None or lane.index == 0 else (lane.index, tp)

    async def verify_topics(self):
        """Refuse to start when an input, output or dead-letter topic is missing."""
        wanted: dict[str, set] = {}
        for w in self.workers:
            bootstrap = w.bootstrap_servers or self.settings.kafka_bootstrap_servers
            topics = wanted.setdefault(bootstrap, set())
            topics.update(w.topics_in, w.topics_out)
            topics.add(w.retry_to_dlq.dlq_topic if w.retry_to_dlq else self.settings.error_topic)
        missing = set()
        for bootstrap, topics in wanted.items():
            admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap, **self.security())
            await admin.start()
            try:
                # Broker metadata can lag a fresh topic creation: re-read before refusing.
                for attempt in range(3):
                    absent = topics - set(await admin.list_topics())
                    if not absent:
                        break
                    await asyncio.sleep(0.5 * (attempt + 1))
            finally:
                await admin.close()
            missing.update(absent)
        if missing:
            raise MissingTopicsError(missing)

    async def _start(self):
        for module in self.settings.worker_modules:
            importlib.import_module(module)
        self.workers = validate_workers(all_workers() if self.workers is None else self.workers)
        self.workers = self.select_workers(self.workers)
        if not self.workers:
            raise RuntimeError("No Kafka workers registered")
        if self.settings.verify_topics:
            await self.verify_topics()
        self.active = {w.name: set() for w in self.workers}
        self.admission = {w.name: Admission(w) for w in self.workers}
        self.executors = {
            w.name: ThreadPoolExecutor(w.max_workers, thread_name_prefix=f"qf-{w.name}")
            for w in self.workers
            if not inspect.iscoroutinefunction(w.fn)
        }
        lanes: dict[tuple, Lane] = {}
        for w in self.workers:
            key = self.lane_key(w)
            lanes.setdefault(key, Lane(*key)).workers.append(w)
        default_key = (
            self.settings.kafka_bootstrap_servers,
            self.settings.consumer_name,
            "earliest",
        )
        # The default lane comes first: it is the 2.0 consumer (or the injected one).
        self.lanes = sorted(
            lanes.values(),
            key=lambda lane: (lane.bootstrap, lane.group, lane.offset_reset) != default_key,
        )
        for index, lane in enumerate(self.lanes):
            lane.index = index
            for w in lane.workers:
                for topic in w.topics_in:
                    if topic in lane.by_topic:
                        raise RuntimeError(
                            f"Topic '{topic}' already handled by '{lane.by_topic[topic].name}' "
                            f"in consumer group '{lane.group}'"
                        )
                    lane.by_topic[topic] = w
        # 2.0 attribute: the first lane's topic owners.
        self.by_topic = dict(self.lanes[0].by_topic)
        for index, lane in enumerate(self.lanes):
            injected = index == 0 and self.consumer is not None
            lane.consumer = self.consumer if injected else self.new_consumer(lane)
            lane.coordinator = CommitCoordinator(
                lane.consumer, self.metrics, self.settings.commit_timeout_sec
            )
            topics = [t for w in lane.workers for t in w.topics_in]
            lane.consumer.subscribe(topics, listener=RebalanceListener(self, lane))
        self.consumer = self.lanes[0].consumer
        self.coordinator = self.lanes[0].coordinator
        if self.producer is None:
            self.producer = self.new_producer(self.settings.kafka_bootstrap_servers)
        await self.connect(self.producer)
        for bootstrap in sorted(
            {lane.bootstrap for lane in self.lanes} - {self.settings.kafka_bootstrap_servers}
        ):
            self.producers[bootstrap] = self.new_producer(bootstrap)
            await self.connect(self.producers[bootstrap])
        if self.settings.idempotency_enabled or any(w.kind == "aggregator" for w in self.workers):
            if self.aggregation is None:
                from ..redis.aggregation import AggregationStore

                self.aggregation = AggregationStore(self.settings)
            await self.aggregation.start()
        self.processor = Processor(
            self.settings,
            self.producer,
            self.metrics,
            self.executors,
            self.aggregation,
            producers=self.producers,
            observer=self.observer,
        )
        for lane in self.lanes:
            await self.connect(lane.consumer)
        self.ready = True
        self.metrics.ready.set(1)
        self.task = asyncio.create_task(self.run(), name="qf-kafka-runtime")

    def valid(self, key, partition):
        return not partition.revoked and self.partitions.get(key) is partition

    def spec_of(self, key, partition):
        return partition.spec or self.by_topic[(partition.tp or key).topic]

    def schedule(self, key, partition, *, flush=False):
        if partition.revoked or partition.draining or not partition.pending:
            return
        spec = self.spec_of(key, partition)
        tp = key
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
            generation = admission.acquire()
            if generation is None:
                break
            n = min(len(partition.pending), spec.batch_size) if spec.bulk_mode else 1
            records = [partition.pending.popleft()[0] for _ in range(n)]
            task = asyncio.create_task(
                self.job(spec, tp, partition, records, generation), name=f"qf-{spec.name}"
            )
            active.add(task)
            partition.tasks.add(task)

            def done(
                future,
                active=active,
                partition=partition,
                admission=admission,
                generation=generation,
            ):
                active.discard(future)
                partition.tasks.discard(future)
                self.wakeup.set()
                if future.cancelled():
                    # A task cancelled before its first step never enters job().
                    admission.abandon(generation)
                if not future.cancelled():
                    error = future.exception()
                    # Stale work is harmless only after its partition was
                    # actually revoked/replaced. Lost Redis ownership while
                    # still assigned otherwise leaves an unfinished offset gap
                    # forever; fail visibly so supervision can replay it.
                    if error is not None and (
                        not isinstance(error, StaleAssignment) or self.valid(key, partition)
                    ):
                        self.error = error
                        self.ready = False
                        self.metrics.ready.set(0)

            task.add_done_callback(done)

    async def job(self, spec, key, partition, records, generation):
        """Advance source progress only after handler/output/ledger completion."""
        self.metrics.inflight.labels(spec.name).inc()
        outcomes = Counter()
        token = JOB_OUTCOMES.set(outcomes)
        started = time.monotonic()
        try:
            ok = await self.processor.handle(spec, records, lambda: self.valid(key, partition))
            if ok is False:
                self.admission[spec.name].failure(generation)
            else:
                self.admission[spec.name].success(generation)
            tp = partition.tp or key
            _, coordinator = self.lane_of(partition)
            coordinator.complete(tp, [r.offset for r in records], partition.progress)
            if self.observer is not None:
                report = JobReport(
                    worker=spec.name,
                    topic=tp.topic,
                    partition=tp.partition,
                    records=len(records),
                    outcomes=dict(outcomes),
                    duration_s=time.monotonic() - started,
                    ok=ok is not False,
                )
                notify(self.observer, "on_job", report)
        except (asyncio.CancelledError, StaleAssignment):
            self.admission[spec.name].abandon(generation)
            raise
        except Exception:
            self.admission[spec.name].failure(generation)
            raise
        finally:
            JOB_OUTCOMES.reset(token)
            self.metrics.inflight.labels(spec.name).dec()

    def enqueue(self, tp, records, lane=None):
        partition = self.partitions.get(self.pkey(lane, tp))
        if partition is None or partition.revoked or partition.draining:
            return
        progress = partition.progress
        consumer, coordinator = self.lane_of(partition)
        for record in records:
            if len(progress.offsets) >= self.settings.pending_max_per_tp:
                consumer.pause(tp)
                consumer.seek(tp, record.offset)
                break
            coordinator.register(tp, record.offset)
            partition.pending.append((record, time.monotonic()))

    def lane_of(self, partition):
        """``(consumer, coordinator)`` owning ``partition``."""
        if partition.lane is not None:
            return partition.lane.consumer, partition.lane.coordinator
        return self.consumer, self.coordinator

    def tick(self, *, flush=False):
        # Rotate order to keep hot partitions from monopolizing shared worker slots.
        for key, partition in list(self.partitions.items()):
            self.schedule(key, partition, flush=flush)
            spec = self.spec_of(key, partition)
            tp = partition.tp or key
            count = len(partition.progress.offsets)
            self.metrics.pending.labels(spec.name, tp.topic, str(tp.partition)).set(
                len(partition.pending)
            )
            self.metrics.uncommitted.labels(tp.topic, str(tp.partition)).set(count)
            admission = self.admission[spec.name]
            self.metrics.circuit.labels(spec.name).set(
                bool(admission.open_until or admission.probe)
            )
            consumer, _ = self.lane_of(partition)
            if not partition.revoked and tp in consumer.assignment():
                if count >= self.settings.pending_max_per_tp:
                    consumer.pause(tp)
                else:
                    consumer.resume(tp)
                highwater = consumer.highwater(tp)
                if highwater is not None and partition.progress.committed is not None:
                    lag = max(0, highwater - partition.progress.committed)
                    self.metrics.lag.labels(tp.topic, str(tp.partition)).set(lag)
                    if self.observer is not None:
                        group = (
                            partition.lane.group if partition.lane else self.settings.consumer_name
                        )
                        notify(self.observer, "on_lag", group, tp.topic, tp.partition, lag)
        if self.partitions:
            key = next(iter(self.partitions))
            self.partitions[key] = self.partitions.pop(key)

    async def commit(self, partitions=None):
        ok = True
        for lane in self.lanes or [None]:
            coordinator = lane.coordinator if lane else self.coordinator
            ok = await coordinator.commit(partitions) and ok
        self.commit_failures = 0 if ok else self.commit_failures + 1
        if self.commit_failures >= self.settings.transport_attempts:
            raise RuntimeError("Commit retry budget exhausted; source offsets remain replayable")

    async def run(self):
        last_commit = time.monotonic()
        polls: dict[int, asyncio.Task] = {}
        polled_at: dict[int, int] = {}
        lanes = self.lanes or [None]
        try:
            while not self.closing:
                if self.error is not None:
                    raise self.error
                self.wakeup.clear()
                self.tick()
                for index, lane in enumerate(lanes):
                    consumer = lane.consumer if lane else self.consumer
                    if index not in polls:
                        polled_at[index] = time.time_ns()
                        polls[index] = asyncio.create_task(
                            consumer.getmany(
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
                        {*polls.values(), wake_task},
                        timeout=timeout,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    wake_task.cancel()
                    await asyncio.gather(wake_task, return_exceptions=True)
                for index, poll_task in list(polls.items()):
                    if poll_task in done:
                        del polls[index]
                        fetched = poll_task.result()
                        if fetched and self.settings.trace_kafka_operations:
                            self.trace_poll(lanes[index], fetched, polled_at[index])
                        for tp, messages in fetched.items():
                            self.enqueue(tp, messages, lanes[index])
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
            for poll_task in polls.values():
                poll_task.cancel()
            await asyncio.gather(*polls.values(), return_exceptions=True)
            self.ready = False
            self.metrics.ready.set(0)

    def trace_poll(self, lane, fetched, started_ns):
        """A ``kafka.poll`` span covering one non-empty fetch (``trace_kafka_operations``)."""
        span = get_tracer().start_span("kafka.poll", kind=SpanKind.CONSUMER, start_time=started_ns)
        span.set_attribute("messaging.system", "kafka")
        span.set_attribute(
            "messaging.consumer.group.name", lane.group if lane else self.settings.consumer_name
        )
        span.set_attribute(
            "messaging.destination.name", ",".join(sorted({tp.topic for tp in fetched}))
        )
        span.set_attribute("messaging.batch.message_count", sum(len(r) for r in fetched.values()))
        span.end()

    async def revoke(self, revoked, lane=None):
        """Drain owned jobs, fence unfinished work, then commit safe prefixes.

        Unstarted and cancelled records stay uncommitted for the next owner.
        A synchronous function already executing in a thread cannot be killed;
        it must make external side effects independently idempotent.
        """
        keys = [self.pkey(lane, tp) for tp in revoked]
        targets = {key: self.partitions[key] for key in keys if key in self.partitions}
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
            for key, p in targets.items():
                _, coordinator = self.lane_of(p)
                await coordinator.commit({p.tp or key})
            for key, p in targets.items():
                _, coordinator = self.lane_of(p)
                tp = p.tp or key
                coordinator.partitions.pop(tp, None)
                self.partitions.pop(key, None)
                spec = self.spec_of(key, p)
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
        coordinators = [lane.coordinator for lane in self.lanes] or [self.coordinator]
        for coordinator in coordinators:
            if coordinator is None:
                continue
            for _ in range(self.settings.transport_attempts):
                if await coordinator.commit():
                    break
        extra = [lane.consumer for lane in self.lanes if lane.consumer is not self.consumer]
        clients = [self.consumer, *extra, self.producer, *self.producers.values(), self.aggregation]
        for client in clients:
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
