"""Isolated, self-restarting Kafka runtimes: one per consumer lane (Supervisor).

A single :class:`~framework.etl.framework_etl.KafkaRuntime` fails as a whole: an
unreachable cluster, a missing topic or a crashed worker stops every worker it runs.
``KafkaSupervisor`` runs one runtime per *unit* (the workers sharing a consumer:
same cluster, consumer group and offset reset) and restarts each unit on its own
with exponential backoff (5, 10, 20, 40, 60, 60 … s by default), so one broken flow
never stops the others.

It honours the runtime contract of ``FrameworkApp(runtime_factory=...)``: async
``start``/``stop``, ``ready``, ``error`` (always ``None``: every failure is
recoverable by a restart, so liveness never kills a pod that is backing off) and
``task``. ``status()`` reports each worker:

=================  =============================================================
``starting``       its runtime is connecting
``running``        consuming
``backing_off``    its runtime failed (unreachable broker, crash); restart pending
``topic_missing``  ``verify_topics`` found a missing topic (detail names it); retried
``disabled``       declared ``enabled=False`` or listed in ``disabled_workers``
``stopped``        the supervisor stopped
=================  =============================================================

On ``stop`` every runtime drains its in-flight jobs for at most
``shutdown_timeout_sec``, commits the completed offsets and leaves its group.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from ..commons.logger import logger
from ..decorators.kafka_workers import WorkerSpec, validate_workers
from ..observability import Metrics
from .framework_etl import KafkaRuntime, MissingTopicsError
from .observer import WorkerObserver, notify

STARTING, RUNNING, BACKING_OFF, TOPIC_MISSING, DISABLED, STOPPED = (
    "starting",
    "running",
    "backing_off",
    "topic_missing",
    "disabled",
    "stopped",
)


@dataclass
class Unit:
    """The workers sharing one consumer, and the state of their runtime."""

    key: tuple
    workers: list[WorkerSpec]
    state: str = STOPPED
    detail: str | None = None
    restarts: int = 0
    runtime: KafkaRuntime | None = None
    task: asyncio.Task | None = field(default=None, repr=False)


class KafkaSupervisor:
    """Runs and restarts one ``KafkaRuntime`` per consumer lane.

    Args:
        settings: Framework settings shared by every runtime.
        workers: The worker set (``collect_workers`` output, or registered workers).
        metrics: Metrics registry shared by every runtime.
        observer: Receives job reports, lag, state changes and restarts.
        restart_base_sec: First restart delay.
        restart_cap_sec: Largest restart delay.
        runtime_factory: ``(settings, workers, metrics, observer) -> KafkaRuntime``-like
            object (tests inject fakes).
        sleep: Awaitable sleep (tests inject a fake clock).
    """

    def __init__(
        self,
        settings,
        workers: Sequence[WorkerSpec],
        *,
        metrics: Metrics | None = None,
        observer: WorkerObserver | None = None,
        restart_base_sec: float = 5.0,
        restart_cap_sec: float = 60.0,
        runtime_factory: Callable[..., Any] | None = None,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ):
        self.settings = settings
        self.metrics = metrics or Metrics()
        self.observer = observer
        self.restart_base_sec, self.restart_cap_sec = restart_base_sec, restart_cap_sec
        self.runtime_factory = runtime_factory or self._default_runtime
        self.sleep = sleep
        specs = validate_workers(workers)
        disabled = set(settings.disabled_workers)
        unknown = disabled - {w.name for w in specs}
        if unknown:
            raise ValueError(
                "disabled_workers names unknown workers: " + ", ".join(sorted(unknown))
            )
        self.disabled = sorted({w.name for w in specs if not w.enabled} | disabled)
        units: dict[tuple, Unit] = {}
        for w in specs:
            if w.name in self.disabled:
                continue
            key = (
                w.bootstrap_servers or settings.kafka_bootstrap_servers,
                w.consumer_group or settings.consumer_name,
                w.auto_offset_reset,
            )
            units.setdefault(key, Unit(key, [])).workers.append(w)
        self.units = list(units.values())
        self.task: asyncio.Task | None = None
        self.error: BaseException | None = None
        self._stopping = False

    def _default_runtime(self, settings, workers, metrics, observer):
        # disabled_workers was applied above; each runtime gets only its unit.
        return KafkaRuntime(
            settings.model_copy(update={"disabled_workers": []}),
            metrics=metrics,
            workers=workers,
            observer=observer,
        )

    # ------------------------------------------------------- runtime contract
    @property
    def ready(self) -> bool:
        """Every enabled worker is running."""
        return bool(self.units) and all(u.state == RUNNING for u in self.units)

    def status(self) -> dict[str, dict[str, Any]]:
        """Per worker: ``state``, ``detail``, ``restarts``, ``consumer_group``."""
        result = {
            name: {"state": DISABLED, "detail": None, "restarts": 0, "consumer_group": None}
            for name in self.disabled
        }
        for unit in self.units:
            for w in unit.workers:
                result[w.name] = {
                    "state": unit.state,
                    "detail": unit.detail,
                    "restarts": unit.restarts,
                    "consumer_group": unit.key[1],
                }
        return result

    async def start(self) -> None:
        """Start supervising every unit; returns at once."""
        if self.task is not None:
            raise RuntimeError("KafkaSupervisor instances can only start once")
        for name in self.disabled:
            notify(self.observer, "on_state", name, DISABLED, None)
        self.task = asyncio.create_task(self._run(), name="qf-kafka-supervisor")

    async def stop(self) -> None:
        """Stop every runtime (each drains and commits), then the supervisor."""
        if self.task is None:
            return
        self._stopping = True
        runtimes = [u.runtime for u in self.units if u.runtime is not None]
        await asyncio.gather(*(r.stop() for r in runtimes), return_exceptions=True)
        self.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.task
        for unit in self.units:
            self._set(unit, STOPPED, None)

    # ------------------------------------------------------------ supervision
    async def _run(self) -> None:
        async with asyncio.TaskGroup() as group:
            for unit in self.units:
                unit.task = group.create_task(self._supervise(unit), name=f"qf-unit-{unit.key[1]}")

    def _set(self, unit: Unit, state: str, detail: str | None) -> None:
        if (unit.state, unit.detail) == (state, detail):
            return
        unit.state, unit.detail = state, detail
        for w in unit.workers:
            notify(self.observer, "on_state", w.name, state, detail)

    async def _supervise(self, unit: Unit) -> None:
        delay = self.restart_base_sec
        names = [w.name for w in unit.workers]
        while not self._stopping:
            self._set(unit, STARTING, None)
            runtime = self.runtime_factory(self.settings, unit.workers, self.metrics, self.observer)
            unit.runtime = runtime
            error: BaseException | None = None
            started = None
            try:
                await runtime.start()
                self._set(unit, RUNNING, None)
                started = asyncio.get_running_loop().time()
                await runtime.task
                error = runtime.error
            except asyncio.CancelledError:
                raise
            except MissingTopicsError as exc:
                error = exc
                self._set(unit, TOPIC_MISSING, ",".join(exc.missing))
            except Exception as exc:
                error = exc
            finally:
                unit.runtime = None
                with contextlib.suppress(Exception):
                    await runtime.stop()
            if self._stopping:
                return
            # A runtime that ran longer than the largest delay was healthy: start over.
            if (
                started is not None
                and asyncio.get_running_loop().time() - started >= self.restart_cap_sec
            ):
                delay = self.restart_base_sec
            if unit.state != TOPIC_MISSING:
                self._set(unit, BACKING_OFF, type(error).__name__ if error else "exited")
            unit.restarts += 1
            logger.error(
                "Kafka workers failed; restarting",
                extra={
                    "event": {
                        "workers": names,
                        "delay_sec": delay,
                        "error_type": type(error).__name__ if error else None,
                        "restarts": unit.restarts,
                    }
                },
            )
            notify(self.observer, "on_restart", names, delay, error)
            await self.sleep(delay)
            delay = min(delay * 2, self.restart_cap_sec)
