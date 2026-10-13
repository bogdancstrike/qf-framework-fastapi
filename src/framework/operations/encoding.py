"""Responses encoded byte for byte like Flask's ``jsonify``.

Flask serialises with ``json.dumps(body)`` (ASCII escapes, ``", "`` / ``": "`` separators)
and appends a newline. Reproducing the bytes keeps shadow comparisons with the legacy
service exact and non-ASCII messages identical for every client.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from starlette.responses import Response


def _default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def dumps(body: Any) -> str:
    """Flask ``jsonify`` encoding: ASCII-escaped JSON plus a trailing newline."""
    return json.dumps(body, ensure_ascii=True, default=_default) + "\n"


class LegacyJSONResponse(Response):
    """JSON response encoded exactly like Flask's ``jsonify``."""

    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        """Encode `content` with `dumps`."""
        return dumps(content).encode("ascii")


def respond(body: Any, status: int = 200, headers: dict[str, str] | None = None) -> Response:
    """A `LegacyJSONResponse` with `status` and `headers`."""
    return LegacyJSONResponse(body, status_code=status, headers=headers)
