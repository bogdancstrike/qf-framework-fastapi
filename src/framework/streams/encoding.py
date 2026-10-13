"""JSON encoders for published values (pick one per worker with ``encoder=``).

The framework's default stays the compact form it has always produced. Services that
replace a Python producer using ``json.dumps(value)`` defaults (``", "``/``": "``
separators, ASCII escapes) select :func:`python_json`, so consumers comparing bytes
or hashing payloads see no change.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

#: Turns a JSON-compatible value into the bytes of a Kafka record value.
Encoder = Callable[[Any], bytes]


def compact_json(value: Any) -> bytes:
    """``{"a":1}``: no spaces, UTF-8, NaN refused (the 2.0 default)."""
    return json.dumps(value, separators=(",", ":"), allow_nan=False).encode()


def python_json(value: Any) -> bytes:
    """``{"a": 1}``: Python ``json.dumps`` defaults, non-ASCII escaped as ``\\uXXXX``."""
    return json.dumps(value, ensure_ascii=True, allow_nan=False).encode()
