"""Route catalogue (Registry): the legacy contract of every operation route in the endpoint maps.

Operation routes are declared like any QF dynamic endpoint (``namespaces``, ``endpoints`` with
``operation_name``, ``request_method``, ``exec_method``) in one map file per route *group*.
The framework compiles the routes (`generate_endpoints_from_config`); the catalogue reads the
same files for what the framework does not model, per endpoint::

    "operation_contract": {"model": "search_body", "body_kind": "validated"}

- ``model``: a name under the map's ``operation_schemas`` (flask-restx Draft 4 validation,
  400 answers). Route models are kept out of the framework's ``models`` key on purpose: that
  key validates with Pydantic (lax types, 422 answers);
- ``body_kind``: what the route does with the body (see `BODY_KINDS`).

Whether a route needs a named caller is not declared here: the operation says it with
`@with_user`. A map may name its own `ApiProfile` with ``"api_profile": "module:Class"``.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

from ..api.dynamic import normalize_path
from .identity import requires_user
from .profiles import ApiProfile, ProfileRegistry
from .validation import ModelValidator, compile_model

BodyKind = Literal["validated", "object", "ignored", "raw", "none"]
#: ``validated``: an object checked against ``model``; ``object``: any object; ``raw``: an
#: object, ``[]`` and ``null`` read as ``{}``; ``ignored``: any JSON, read as ``{}``;
#: ``none``: no body (GET routes).
BODY_KINDS: tuple[str, ...] = ("validated", "object", "ignored", "raw", "none")


class CatalogueError(ValueError):
    """An endpoint map is invalid; raised at start-up."""


@dataclass(frozen=True, slots=True)
class OperationSpec:
    """The immutable legacy contract of one route."""

    group: str
    prefix: str
    operation: str
    path: str
    methods: tuple[str, ...]
    model_name: str | None
    validator: ModelValidator | None
    body_kind: BodyKind
    with_user: bool
    profile: ApiProfile
    handler: str

    @property
    def reads_body(self) -> bool:
        """Whether the media-type and JSON rules apply (every body kind but ``none``)."""
        return self.body_kind != "none"


@dataclass(frozen=True, slots=True)
class EndpointMap:
    """One endpoint map file = one route group."""

    group: str
    path: Path
    document: Mapping[str, Any]


def load_map(path: Path) -> EndpointMap:
    """Read one endpoint map and check the keys the operation layer relies on.

    Raises:
        CatalogueError: the map has no ``group``, or declares framework ``models`` instead of
            ``operation_schemas``.
    """
    with Path(path).open(encoding="utf-8") as fh:
        doc = json.load(fh)
    group = doc.get("group")
    if not group:
        raise CatalogueError(f"{path}: missing 'group'")
    if doc.get("models"):
        raise CatalogueError(
            f"{path}: put route models under 'operation_schemas' "
            "(the framework 'models' key validates with Pydantic)"
        )
    return EndpointMap(group=str(group), path=Path(path), document=doc)


class RouteCatalogue:
    """Registry of operation routes, keyed by route template. Built once, then read-only."""

    def __init__(self, specs: Iterable[OperationSpec]) -> None:
        by_path: dict[str, OperationSpec] = {}
        for spec in specs:
            if spec.path in by_path:
                raise CatalogueError(f"duplicate_operation_route: {spec.path}")
            by_path[spec.path] = spec
        self._by_path = MappingProxyType(by_path)

    @classmethod
    def from_maps(
        cls, maps: Iterable[EndpointMap], profiles: ProfileRegistry | None = None
    ) -> RouteCatalogue:
        """Every operation route of `maps` (duplicate paths fail)."""
        registry = profiles or ProfileRegistry()
        return cls(spec for m in maps for spec in _MapReader(m, registry).specs())

    def get(self, path: str) -> OperationSpec | None:
        """The spec of the route template `path`, or `None`."""
        return self._by_path.get(path)

    def __iter__(self) -> Iterator[OperationSpec]:
        return iter(self._by_path.values())

    def __len__(self) -> int:
        return len(self._by_path)

    @property
    def groups(self) -> tuple[str, ...]:
        """Route groups in map order, without duplicates."""
        return tuple(dict.fromkeys(s.group for s in self._by_path.values()))


def describe_routes(
    catalogue: RouteCatalogue, mounted_groups: Iterable[str] | None = None
) -> list[str]:
    """One line per route (``group method path body_kind with_user``), for a CLI listing."""
    groups = set(mounted_groups) if mounted_groups is not None else None
    lines = []
    for spec in catalogue:
        note = "" if groups is None or spec.group in groups else "  (group not mounted)"
        lines.append(
            f"{spec.group:8} {','.join(spec.methods):5} {spec.path:40} {spec.body_kind:9} "
            f"with_user={str(spec.with_user).lower()}{note}"
        )
    return lines


def _import(target: str, what: str, where: Path) -> Any:
    module_name, _, attr = target.partition(":")
    try:
        return getattr(importlib.import_module(module_name), attr)
    except (ImportError, AttributeError) as exc:
        raise CatalogueError(f"{where}: unknown {what} {target!r}") from exc


class _MapReader:
    """Builds the `OperationSpec`s of one endpoint map."""

    def __init__(self, endpoint_map: EndpointMap, profiles: ProfileRegistry) -> None:
        self.map = endpoint_map
        self.doc = endpoint_map.document
        self.profiles = profiles
        self.validators = self._validators()
        self.namespaces = {ns["name"] for ns in self.doc.get("namespaces", [])}
        self.custom_profile = self._custom_profile()

    def specs(self) -> list[OperationSpec]:
        """One spec per endpoint, in map order."""
        return [self._spec(entry) for entry in self.doc.get("endpoints", [])]

    def _validators(self) -> dict[str, ModelValidator | None]:
        models = self.doc.get("operation_schemas") or {}
        compiled: dict[str, ModelValidator | None] = {}
        for name, definition in models.items():
            schema = compile_model(name, definition, models)
            compiled[name] = ModelValidator(schema) if schema is not None else None
        return compiled

    def _custom_profile(self) -> ApiProfile | None:
        target = self.doc.get("api_profile")
        if not target:
            return None
        profile = _import(str(target), "api_profile", self.map.path)()
        if not isinstance(profile, ApiProfile):
            raise CatalogueError(f"{self.map.path}: {target!r} is not an ApiProfile")
        return profile

    def _spec(self, entry: Mapping[str, Any]) -> OperationSpec:
        ns, op = entry["namespace"], entry["operation_name"]
        if ns not in self.namespaces:
            raise CatalogueError(f"{self.map.path}: unknown namespace {ns!r}")
        if entry.get("model_name") is not None:
            raise CatalogueError(
                f"{self.map.path}: {op}: use operation_contract.model, not model_name"
            )
        model_name, body_kind = self._contract(op, entry.get("operation_contract") or {})
        target = entry["exec_method"]
        handler = f"{target['module_name']}:{target['method_name']}"
        return OperationSpec(
            group=self.map.group,
            prefix=ns,
            operation=op,
            path=normalize_path(ns, op, entry.get("api_url"))[0],
            methods=tuple(m.upper() for m in entry["request_method"]),
            model_name=model_name,
            validator=self.validators.get(model_name) if model_name else None,
            body_kind=body_kind,
            with_user=requires_user(_import(handler, "handler", self.map.path)),
            profile=self.custom_profile or self.profiles.for_prefix(ns),
            handler=handler,
        )

    def _contract(self, op: str, contract: Mapping[str, Any]) -> tuple[str | None, BodyKind]:
        model_name = contract.get("model")
        if model_name is not None and model_name not in self.validators:
            raise CatalogueError(f"{self.map.path}: {op}: unknown operation schema {model_name!r}")
        body_kind = contract.get("body_kind", "validated" if model_name else "object")
        if body_kind not in BODY_KINDS:
            raise CatalogueError(f"{self.map.path}: {op}: unknown body_kind {body_kind!r}")
        return model_name, body_kind
