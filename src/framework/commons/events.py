"""Event logging: `log.info("event_name", key=value)` with redaction, on the framework's JSON lines.

`JSONFormatter` (``framework.commons.logger``) writes one JSON line per record and merges
``record.event`` into it. This module adds the application-facing side:

- `EventLogger`: an adapter whose first argument is a stable snake_case event name and whose
  keyword arguments become JSON fields;
- `redact`: a log-safe copy of any value. Values under a redaction key become
  ``"<redacted len=N>"``, long strings ``"<truncated len=N>"``, containers are walked;
- `scrub`: DSN passwords (``scheme://user:***@host``) and URL signatures or tokens hidden in
  free text;
- `ScrubbingJSONFormatter`: the framework's JSON line with tracebacks passed through `scrub`
  (exception messages can carry a DSN or a presigned URL);
- `configure_event_logging(root, level)`: install the formatter on an application's logger tree.

Example::

    from framework.commons.events import configure_event_logging, get_event_logger

    configure_event_logging("myapp", "INFO")
    log = get_event_logger("myapp.orders")
    log.info("order_accepted", order_id=42, payload=body)   # payload is redacted
"""

from __future__ import annotations

import logging
import re
from collections.abc import Collection, Mapping, MutableMapping
from typing import Any

from .logger import JSONFormatter
from .logger import configure_logging as configure_framework_logging

#: Keys whose values never reach a log line (document bodies, prompts, credentials).
DEFAULT_REDACTED_KEYS = frozenset(
    {
        "doc", "document", "documents", "message", "messages", "payload", "content", "text",
        "Text", "prompt", "body", "value", "item", "items", "password", "secret", "token",
        "authorization", "api_key", "access_key", "secret_key", "credentials", "cookie",
        "input", "query_text",
    }
)  # fmt: skip
#: Strings longer than this are replaced by their length.
MAX_STRING = 256
#: Containers deeper than this are cut (``"<nested>"``).
MAX_DEPTH = 4
#: Items of a list shown before ``"<+N more>"``.
MAX_ITEMS = 20
#: Libraries kept at WARNING unless the level is DEBUG.
NOISY_LIBRARIES = ("elasticsearch", "elastic_transport", "aiokafka", "httpx", "httpcore", "asyncio")

_DSN_PASSWORD = re.compile(r"(?P<head>[A-Za-z][A-Za-z0-9+.-]*://[^\s:/@]+:)[^\s@/]+@")
_URL_SECRET = re.compile(
    r"(?P<head>[?&](?:X-Amz-(?:Signature|Credential|Security-Token)"
    r"|Signature|token|access_token|api_key|apikey)=)[^&\s\"']+",
    re.IGNORECASE,
)
_STDLIB_OPTIONS = ("exc_info", "stack_info", "stacklevel")


def scrub(text: str) -> str:
    """`text` with DSN passwords and URL signatures/tokens replaced by ``***``."""
    if "://" not in text and "=" not in text:
        return text
    text = _DSN_PASSWORD.sub(r"\g<head>***@", text)
    return _URL_SECRET.sub(r"\g<head>***", text)


def _length(value: Any) -> int:
    try:
        return len(value)
    except TypeError:
        return len(str(value))


def _redact_sequence(items: list[Any], keys: Collection[str], depth: int) -> list[Any]:
    shown = [redact(v, None, depth=depth + 1, keys=keys) for v in items[:MAX_ITEMS]]
    if len(items) > MAX_ITEMS:
        shown.append(f"<+{len(items) - MAX_ITEMS} more>")
    return shown


def redact(
    value: Any,
    key: str | None = None,
    *,
    depth: int = 0,
    keys: Collection[str] = DEFAULT_REDACTED_KEYS,
) -> Any:
    """A log-safe copy of `value`.

    Args:
        value: Anything a caller wants to log.
        key: The field name `value` is logged under (a redaction key hides it).
        depth: Current nesting level (internal).
        keys: The redaction keys.
    """
    if key is not None and key in keys and value is not None:
        return f"<redacted len={_length(value)}>"
    if isinstance(value, str):
        return scrub(value) if len(value) <= MAX_STRING else f"<truncated len={len(value)}>"
    if depth >= MAX_DEPTH:
        return "<nested>"
    if isinstance(value, Mapping):
        return {str(k): redact(v, str(k), depth=depth + 1, keys=keys) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return _redact_sequence(list(value), keys, depth)
    return value


class EventLogger(logging.LoggerAdapter):
    """Adapter (EventLogger): ``log.info("event_name", key=value)``; fields become JSON keys.

    Fields are redacted (`redact`) before they reach a handler. ``exc_info``, ``stack_info``
    and ``stacklevel`` keep their stdlib meaning. `bind` returns a child logger that adds
    fields to every line.

    Args:
        logger: The stdlib logger to write to.
        extra: Fields added to every line.
        redacted_keys: The redaction keys (default `DEFAULT_REDACTED_KEYS`).
    """

    def __init__(
        self,
        logger: logging.Logger,
        extra: Mapping[str, Any] | None = None,
        *,
        redacted_keys: Collection[str] = DEFAULT_REDACTED_KEYS,
    ) -> None:
        super().__init__(logger, dict(extra or {}))
        self.redacted_keys = redacted_keys

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        """Move the keyword fields into ``extra["event"]`` (redacted); keep the stdlib options."""
        std = {k: kwargs.pop(k) for k in _STDLIB_OPTIONS if k in kwargs}
        fields = redact({**(self.extra or {}), **kwargs}, keys=self.redacted_keys)
        std["extra"] = {"event": {"event": str(msg), **fields}}
        return msg, std

    def debug(self, msg: object, *args: object, **fields: Any) -> None:
        """Log the event `msg` at DEBUG with `fields`."""
        self.log(logging.DEBUG, msg, *args, **fields)

    def info(self, msg: object, *args: object, **fields: Any) -> None:
        """Log the event `msg` at INFO with `fields`."""
        self.log(logging.INFO, msg, *args, **fields)

    def warning(self, msg: object, *args: object, **fields: Any) -> None:
        """Log the event `msg` at WARNING with `fields`."""
        self.log(logging.WARNING, msg, *args, **fields)

    def error(self, msg: object, *args: object, **fields: Any) -> None:
        """Log the event `msg` at ERROR with `fields`."""
        self.log(logging.ERROR, msg, *args, **fields)

    def exception(self, msg: object, *args: object, exc_info: Any = True, **fields: Any) -> None:
        """Log the event `msg` at ERROR with `fields` and the current exception."""
        self.log(logging.ERROR, msg, *args, exc_info=exc_info, **fields)

    def critical(self, msg: object, *args: object, **fields: Any) -> None:
        """Log the event `msg` at CRITICAL with `fields`."""
        self.log(logging.CRITICAL, msg, *args, **fields)

    def bind(self, **fields: Any) -> EventLogger:
        """A child logger that adds `fields` to every line."""
        return EventLogger(
            self.logger, {**(self.extra or {}), **fields}, redacted_keys=self.redacted_keys
        )


class ScrubbingJSONFormatter(JSONFormatter):
    """The framework's JSON line, with formatted tracebacks passed through `scrub`."""

    def formatException(self, ei: Any) -> str:
        """The stdlib traceback text, scrubbed."""
        return scrub(super().formatException(ei))


def get_event_logger(
    name: str, *, redacted_keys: Collection[str] = DEFAULT_REDACTED_KEYS
) -> EventLogger:
    """An `EventLogger` writing to the stdlib logger `name`."""
    return EventLogger(logging.getLogger(name), redacted_keys=redacted_keys)


def configure_event_logging(
    root: str, level: str = "INFO", *, quiet: Collection[str] = NOISY_LIBRARIES
) -> None:
    """Install JSON event logging on the logger tree `root` (and the framework's). Idempotent.

    Args:
        root: The application's root logger name (e.g. ``"myapp"``).
        level: Level of `root` and the framework logger.
        quiet: Libraries kept at WARNING unless `level` is DEBUG.
    """
    configure_framework_logging(level)
    logger = logging.getLogger(root)
    if not any(getattr(h, "_qf_events", False) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler._qf_events = True  # type: ignore[attr-defined]
        handler.setFormatter(ScrubbingJSONFormatter())
        logger.addHandler(handler)
    logger.setLevel(level.upper())
    logger.propagate = False
    library_level = logging.DEBUG if level.upper() == "DEBUG" else logging.WARNING
    for name in quiet:
        logging.getLogger(name).setLevel(library_level)
