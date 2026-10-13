"""Request-model validation with flask-restx semantics (Draft 4, 400 answers).

Route models are declared in the endpoint maps under ``operation_schemas``, in the QF field
notation (``{"type": "string", "args": {"required": true}}``). Each model is compiled once
to the JSON schema flask-restx would generate (``Model.__schema__``) and checked with a
Draft 4 validator, so status codes, keys and messages match what legacy clients receive:

- types are strict (``"true"`` is not a boolean, ``2.0`` is not an integer), ``null`` is rejected;
- enums are case-sensitive; unknown keys are allowed; defaults and examples are inert;
- ``{"type": "nested", "model": "<name>"}`` and lists of them inline another model, the way
  flask-restx validates ``fields.Nested``;
- ``args.enum_ci`` (with optional ``args.enum_ci_aliases``) is the one extension: an enum
  matched case-insensitively after stripping, whose message lists the canonical values, so a
  route can accept spelling variants and still answer every other value like the old enum;
- errors are keyed like flask-restx ``format_error``: the dotted path, plus the property
  name for ``required`` errors.

Needs the ``operations`` extra (``jsonschema``).
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import Any

from jsonschema import Draft4Validator, ValidationError, validators

from .werkzeug_answers import VALIDATION_MESSAGE

_SCALAR_TYPES = {
    "string": "string",
    "integer": "integer",
    "boolean": "boolean",
    "number": "number",
    "float": "number",
    "dict": "object",
    "object": "object",
    "raw": "object",
}
_REQUIRED = re.compile(r"^'(.*)' is a required property$")
ENUM_CI = "x-enum-ci"


def _enum_ci(
    validator: Any, spec: Mapping[str, list[str]], instance: Any, schema: Any
) -> Iterator[ValidationError]:
    accepted = {c.lower() for c in (*spec["choices"], *spec["aliases"])}
    if not (isinstance(instance, str) and instance.strip().lower() in accepted):
        yield ValidationError(f"{instance!r} is not one of {spec['choices']!r}")


_Validator = validators.extend(Draft4Validator, {ENUM_CI: _enum_ci})


class ModelDefinitionError(ValueError):
    """An operation schema in an endpoint map is malformed (fails start-up)."""


def _nested_schema(
    name: str, ref: Any, models: Mapping[str, Any], seen: tuple[str, ...]
) -> dict[str, Any]:
    if not isinstance(ref, str) or ref not in models:
        raise ModelDefinitionError(f"field {name!r}: unknown nested model {ref!r}")
    if ref in seen:
        raise ModelDefinitionError(f"field {name!r}: recursive nested model {ref!r}")
    return _object_schema(ref, models[ref] or {}, models, (*seen, ref))


def _list_schema(
    name: str, entry: Mapping[str, Any], models: Mapping[str, Any], seen: tuple[str, ...]
) -> dict[str, Any]:
    sub = str(entry.get("super", "string")).lower()
    if sub == "nested":
        items = _nested_schema(name, entry.get("model"), models, seen)
    elif sub in _SCALAR_TYPES:
        items = {"type": _SCALAR_TYPES[sub]}
    else:
        raise ModelDefinitionError(f"field {name!r}: unknown list item type {sub!r}")
    return {"type": "array", "items": items}


def _type_schema(
    name: str, entry: Mapping[str, Any], models: Mapping[str, Any], seen: tuple[str, ...]
) -> dict[str, Any]:
    kind = str(entry.get("type", "string")).lower()
    if kind == "nested":
        return _nested_schema(name, entry.get("model"), models, seen)
    if kind == "list":
        return _list_schema(name, entry, models, seen)
    if kind in _SCALAR_TYPES:
        return {"type": _SCALAR_TYPES[kind]}
    raise ModelDefinitionError(f"field {name!r}: unknown type {kind!r}")


def _field_schema(
    name: str, entry: Mapping[str, Any], models: Mapping[str, Any], seen: tuple[str, ...]
) -> dict[str, Any]:
    schema = _type_schema(name, entry, models, seen)
    args = entry.get("args") or {}
    if args.get("enum"):
        schema["enum"] = list(args["enum"])
    if args.get("enum_ci"):
        schema[ENUM_CI] = {
            "choices": list(args["enum_ci"]),
            "aliases": list(args.get("enum_ci_aliases") or ()),
        }
    return schema


def _object_schema(
    name: str, definition: Mapping[str, Any], models: Mapping[str, Any], seen: tuple[str, ...]
) -> dict[str, Any]:
    if not isinstance(definition, Mapping):
        raise ModelDefinitionError(f"model {name!r} must be an object")
    properties = {
        field: _field_schema(field, entry or {}, models, seen)
        for field, entry in definition.items()
    }
    required = sorted(f for f, e in definition.items() if (e or {}).get("args", {}).get("required"))
    schema: dict[str, Any] = {"properties": properties, "type": "object"}
    if required:
        schema["required"] = required
    return schema


def compile_model(
    name: str, definition: Mapping[str, Any] | None, models: Mapping[str, Any] | None = None
) -> dict[str, Any] | None:
    """The flask-restx JSON schema of a model; `None` means "no model".

    `models` resolves nested fields by name.
    """
    if definition is None:
        return None
    return _object_schema(name, definition, models or {name: definition}, (name,))


class ModelValidator:
    """A compiled, immutable validator for one route model."""

    __slots__ = ("_validator", "schema")

    def __init__(self, schema: Mapping[str, Any]) -> None:
        self.schema = MappingProxyType(dict(schema))
        Draft4Validator.check_schema(dict(schema))
        self._validator = _Validator(dict(schema))

    def errors(self, body: Any) -> dict[str, str]:
        """``{dotted path: message}``; empty when valid. A later error on the same key wins."""
        out: dict[str, str] = {}
        for error in self._validator.iter_errors(body):
            out[_error_key(error)] = error.message
        return out


def _error_key(error: ValidationError) -> str:
    path = [str(p) for p in error.path]
    if error.validator == "required":
        match = _REQUIRED.match(error.message)
        if match:
            path.append(match.group(1))
    return ".".join(path)


#: "The body must be a JSON object" (body kinds ``validated``/``object``/``raw``).
OBJECT_ONLY = ModelValidator({"type": "object"})


def validation_body(errors: Mapping[str, str]) -> dict[str, Any]:
    """The flask-restx validation error body: ``{"errors": {...}, "message": ...}``."""
    return {"errors": dict(errors), "message": VALIDATION_MESSAGE}
