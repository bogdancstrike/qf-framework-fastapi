"""Errors an operation raises, and `ErrorMapper`, which turns any exception into its legacy answer.

- `OperationError(status, body)`: an answer with an exact legacy body and status;
- `ClientInputError(path, reason)`: a request shape the route model let through but the
  business rules reject; answered in the route's `ApiProfile` dialect;
- anything else: logged with its traceback, answered ``{"message": "Internal Server Error"}``.

Applications add their own exception types with `ErrorMapper.register` (most specific class
wins), so their domain stays out of the framework::

    mapper = ErrorMapper().register(AccessDenied, lambda exc, spec: ({"error": "denied"}, 403))
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from starlette.responses import Response

from ..commons.events import EventLogger, get_event_logger
from .encoding import respond
from .werkzeug_answers import INTERNAL_ERROR

if TYPE_CHECKING:
    from .catalogue import OperationSpec


class OperationError(Exception):
    """An error with an exact HTTP body and status (the legacy contract)."""

    def __init__(self, status: int, body: Any, *, log_message: str | None = None) -> None:
        super().__init__(log_message or f"{status}: {body}")
        self.status = status
        self.body = body


class ClientInputError(Exception):
    """An invalid request shape at a dotted field `path`, answered by the route's API profile."""

    def __init__(self, path: str, reason: str = "invalid") -> None:
        super().__init__(f"{path}: {reason}")
        self.path = path
        self.reason = reason


#: ``(exception, spec) -> (body, status)``: the answer to one exception type.
ExceptionAnswer = Callable[[BaseException, "OperationSpec"], tuple[Any, int]]


class ErrorMapper:
    """Chain of exception answers (ErrorMapper): registered types first, then the built-ins.

    Args:
        log: Where answered and unhandled errors are logged (default ``framework.operations``).
    """

    def __init__(self, log: EventLogger | None = None) -> None:
        self._answers: dict[type[BaseException], ExceptionAnswer] = {}
        self._log = log or get_event_logger("framework.operations")

    def register(self, exc_type: type[BaseException], answer: ExceptionAnswer) -> ErrorMapper:
        """Answer `exc_type` (and its subclasses) with `answer`; returns the mapper."""
        self._answers[exc_type] = answer
        return self

    def map(self, exc: BaseException, spec: OperationSpec) -> Response:
        """The legacy answer to an exception raised by a link or the operation."""
        answer = self._registered(exc)
        if answer is not None:
            return respond(*answer(exc, spec))
        if isinstance(exc, OperationError):
            self._log.info("route_error", route=spec.path, status=exc.status, reason=str(exc)[:250])
            return respond(exc.body, exc.status)
        if isinstance(exc, ClientInputError):
            self._log.info(
                "client_input_rejected", route=spec.path, path=exc.path, reason=exc.reason
            )
            return respond(*spec.profile.client_input_error(exc.path, exc.reason))
        self._log.error("route_failed", route=spec.path, error=type(exc).__name__, exc_info=exc)
        return respond(INTERNAL_ERROR, 500)

    def _registered(self, exc: BaseException) -> ExceptionAnswer | None:
        for cls in type(exc).__mro__:
            if cls in self._answers:
                return self._answers[cls]
        return None
