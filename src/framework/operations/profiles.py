"""API profiles: how each legacy API family reports a client-input error (Strategy).

A handler raises `ClientInputError(path, reason)` for a request shape the permissive route
model let through but the business rules reject; a body that is not a JSON object where the
route needs one is answered by `ApiProfile.non_object_body`. How either is answered depends on
the API family the route belongs to, chosen by its URL prefix (the endpoint-map namespace):

- `ValidationBodyProfile`: the flask-restx validation body ``{"errors": {path: reason}, ...}``;
- `ErrorFieldProfile`: one message under one key, e.g. ``{"error": "Invalid request: <path>"}``.

A new family is a new `ApiProfile` registered in the application's `ProfileRegistry` (or
named by a map's ``"api_profile": "module:Class"``); the request chain is not edited.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from .validation import validation_body


class ApiProfile:
    """Strategy (ApiProfile): the client-input error dialect of one legacy API family."""

    def client_input_error(self, path: str, reason: str) -> tuple[Any, int]:
        """The ``(body, status)`` answering a client-input error at `path` (a dotted field path)."""
        raise NotImplementedError

    def non_object_body(self, body: Any) -> tuple[Any, int] | None:
        """The answer to a JSON body that is not an object where the route needs one.

        `None` (the default) keeps the flask-restx answer: the validation body saying the
        body must be an object. A family whose legacy service answered differently (e.g.
        Werkzeug's 400 for a JSON ``null``) overrides it.
        """
        return None


class ValidationBodyProfile(ApiProfile):
    """Answers with the flask-restx validation body ``{path: reason}``, 400."""

    def client_input_error(self, path: str, reason: str) -> tuple[Any, int]:
        """The flask-restx validation body with ``{path: reason}``, 400."""
        return validation_body({path: reason}), 400


class ErrorFieldProfile(ApiProfile):
    """Answers with one formatted message under one key, 400.

    Args:
        key: The body key, e.g. ``"error"``.
        template: Formatted with ``path`` and ``reason``, e.g. ``"Invalid request: {path}"``.
    """

    def __init__(self, key: str = "error", template: str = "Invalid request: {path}") -> None:
        self.key = key
        self.template = template

    def client_input_error(self, path: str, reason: str) -> tuple[Any, int]:
        """``{key: template}``, 400."""
        return {self.key: self.template.format(path=path, reason=reason)}, 400


class ProfileRegistry:
    """Registry (ProfileRegistry): the profile of each URL prefix, frozen once built.

    Args:
        profiles: Profiles by URL prefix (the endpoint-map namespace).
        default: The profile of any other prefix; `None` makes an unknown prefix an error.
    """

    def __init__(
        self,
        profiles: Mapping[str, ApiProfile] | None = None,
        *,
        default: ApiProfile | None = ValidationBodyProfile(),
    ) -> None:
        self._profiles = MappingProxyType(dict(profiles or {}))
        self._default = default

    def for_prefix(self, prefix: str) -> ApiProfile:
        """The profile of `prefix` (`ValueError` when there is none and no default)."""
        profile = self._profiles.get(prefix, self._default)
        if profile is None:
            raise ValueError(f"No API profile for prefix {prefix!r}")
        return profile
