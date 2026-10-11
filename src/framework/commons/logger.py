"""Opt-in JSON logging; no global logger class or handlers installed at import."""

import json
import logging
from contextvars import ContextVar
from datetime import datetime, timezone

from opentelemetry import trace

request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
logger = logging.getLogger("framework")


class JSONFormatter(logging.Formatter):
    def format(self, record):
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if request_id.get():
            event["request_id"] = request_id.get()
        ctx = trace.get_current_span().get_span_context()
        if ctx.is_valid:
            event.update(trace_id=f"{ctx.trace_id:032x}", span_id=f"{ctx.span_id:016x}")
        event.update(getattr(record, "event", {}))
        if record.exc_info:
            event["exception"] = self.formatException(record.exc_info)
        return json.dumps(event, default=str)


def configure_logging(level="INFO"):
    if not any(getattr(h, "_qf", False) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler._qf = True
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
    logger.setLevel(level.upper())
    logger.propagate = False
