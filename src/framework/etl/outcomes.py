"""Per-message outcomes: what a handler decided for each message it received.

Returning plain dicts (or ``None``) keeps the 2.0 contract: the whole job succeeds or
fails together. A handler that returns :class:`Outcome` objects instead decides per
message, which bulk handlers need when one item of a batch is bad:

=================  ===========================================================
``Outcome.ok``     publish the given documents (zero or more) to ``topics_out``
``Outcome.skip``   done, nothing published (a business filter)
``Outcome.dead_letter``  publish a dead letter now (no handler retry)
``Outcome.retry``  invoke the handler again for this message only, within the
                   worker's attempt budget; dead-lettered when it is exhausted
=================  ===========================================================

A single-message handler returns one ``Outcome``; a bulk handler returns a list with
exactly one ``Outcome`` per received message, in the same order (a different count
is a programming error and dead-letters the whole call with reason
``outcome_count_mismatch``). The source record is committed only when every message
it carries has a terminal outcome and every publication was acknowledged.

Outcome mode also changes how unsendable outputs are handled: a document the broker
refuses permanently (too large, unknown or unauthorised topic) dead-letters *its*
message with stage ``forward`` and header ``target_topic`` instead of failing the
runtime, so one oversized document never blocks a partition.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

OutcomeKind = Literal["ok", "skip", "dead_letter", "retry"]


@dataclass(frozen=True, slots=True)
class Outcome:
    """The decision for one message (Value object; build it with the class methods).

    Attributes:
        kind: ``ok``, ``skip``, ``dead_letter`` or ``retry``.
        outputs: Documents published to every output topic (``ok`` only).
        reason: Why the message was skipped, dead-lettered or retried.
        stage: Pipeline stage named in the dead letter (``dead_letter`` only).
        dlq_values: Replayable dead-letter values; empty means the source value.
        dlq_headers: Extra headers of this message's dead letters.
        label: Free-form outcome label counted in ``qf_messages_total`` (e.g. ``stored``).
    """

    kind: OutcomeKind
    outputs: tuple[dict[str, Any], ...] = ()
    reason: str | None = None
    stage: str = "handler"
    dlq_values: tuple[Any, ...] = ()
    dlq_headers: Mapping[str, str] = field(default_factory=dict)
    label: str | None = None

    @classmethod
    def ok(cls, *outputs: dict[str, Any], label: str | None = None) -> Outcome:
        """Processed; publish ``outputs`` (possibly none) to every output topic."""
        for output in outputs:
            if not isinstance(output, dict):
                raise TypeError("Outcome.ok outputs must be dicts")
        return cls("ok", outputs=tuple(outputs), label=label)

    @classmethod
    def skip(cls, reason: str, *, label: str | None = None) -> Outcome:
        """Processed and intentionally not published (filtering is a success)."""
        return cls("skip", reason=reason, label=label)

    @classmethod
    def dead_letter(
        cls,
        reason: str,
        *,
        stage: str = "handler",
        values: tuple[Any, ...] | list[Any] = (),
        headers: Mapping[str, str] | None = None,
        label: str | None = None,
    ) -> Outcome:
        """Not processable; publish dead letters now.

        Args:
            reason: Error text (dead-letter formats may truncate it).
            stage: Pipeline stage that failed (``decode``, ``route``, ``persist``…).
            values: Replayable units to dead-letter; empty dead-letters the source value.
            headers: Extra headers for these dead letters.
            label: Outcome label for metrics.
        """
        return cls(
            "dead_letter",
            reason=reason,
            stage=stage,
            dlq_values=tuple(values),
            dlq_headers=dict(headers or {}),
            label=label,
        )

    @classmethod
    def retry(cls, reason: str | None = None) -> Outcome:
        """Try this message again (counts against the worker's attempt budget)."""
        return cls("retry", reason=reason)

    @property
    def terminal(self) -> bool:
        """Every kind but ``retry`` ends the message."""
        return self.kind != "retry"


def is_outcome_result(result: Any) -> bool:
    """``True`` when a handler result uses the outcome contract."""
    if isinstance(result, Outcome):
        return True
    return isinstance(result, list) and bool(result) and all(isinstance(x, Outcome) for x in result)
