"""Observer hook: lets an application keep its own metrics and status (Observer).

The runtime calls a ``WorkerObserver`` on its event loop at the moments an
ingestion service usually reports: a job finished (with per-label outcome
counts), a worker waits on a transient failure or recovered from one, a
partition's lag changed, and (from :class:`~framework.etl.supervisor.KafkaSupervisor`)
a worker's state changed or its runtime is restarting.

Subclass :class:`WorkerObserver` and override what you need; every method is a
no-op by default. Observers must be quick and must not block: an exception raised
by an observer is logged and ignored, never allowed to affect processing.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass

from ..commons.logger import logger


@dataclass(frozen=True)
class JobReport:
    """One finished job (a single message, or a bulk batch).

    Attributes:
        worker: Worker name.
        topic: Source topic.
        partition: Source partition.
        records: Kafka records in the job.
        outcomes: Count per outcome label (``success``, ``skipped``, ``dlq``, ``duplicate``,
            ``Outcome(label=...)`` values …).
        duration_s: Wall time of the job, retries and publications included.
        ok: ``False`` when the job ended in an application failure (dead letters
            after exhausted attempts); ``True`` otherwise.
    """

    worker: str
    topic: str
    partition: int
    records: int
    outcomes: Mapping[str, int]
    duration_s: float
    ok: bool


class WorkerObserver:
    """Base observer: every hook is a no-op (override the ones you need)."""

    def on_job(self, report: JobReport) -> None:
        """A job finished and its source records are complete."""

    def on_transient(self, worker: str, wait: int, delay_s: float, error: BaseException) -> None:
        """A ``transient`` failure: the worker waits ``delay_s`` before retrying (``wait`` from 0)."""

    def on_recovered(self, worker: str, waits: int) -> None:
        """The handler succeeded after ``waits`` transient waits."""

    def on_lag(self, group: str, topic: str, partition: int, lag: int) -> None:
        """Highwater minus committed offset of an assigned partition (each scheduler tick)."""

    def on_state(self, worker: str, state: str, detail: str | None) -> None:
        """Supervisor: ``starting``, ``running``, ``backing_off``, ``topic_missing``, ``disabled``, ``stopped``."""

    def on_restart(self, workers: list[str], delay_s: float, error: BaseException | None) -> None:
        """Supervisor: the runtime of ``workers`` failed and restarts after ``delay_s``."""


#: Outcome counts of the job running in the current task (set by the runtime).
JOB_OUTCOMES: ContextVar[Counter | None] = ContextVar("qf_job_outcomes", default=None)


def notify(observer: WorkerObserver | None, hook: str, *args) -> None:
    """Call ``observer.<hook>(*args)``; log and ignore its errors."""
    if observer is None:
        return
    try:
        getattr(observer, hook)(*args)
    except Exception:
        logger.exception("Worker observer failed", extra={"event": {"hook": hook}})
