"""Caller identity from gateway headers, and `@with_user` for operations that need a named caller.

Behind an authenticating gateway (QSINT's Gate), the gateway verifies the user and forwards
a user header and a comma-separated roles header. It never forwards the token, so the
headers *are* the identity; the network policy must make sure only the gateway reaches the
service. Identity never comes from the request body.

`GateHeaders` names the headers (default ``X-User-Name`` / ``X-User-Roles``). `with_user` is
the QSINT services' decorator: without a user it answers 400
``{"error": "Missing X-User-Name header"}``; `user_required` builds one with another answer.

Example::

    @api_operation
    @with_user
    async def save_search(call: OperationCall) -> dict:
        return {"owner": call.identity.user}
"""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar


@dataclass(frozen=True, slots=True)
class GateHeaders:
    """The (lower-case) request headers the gateway puts the identity in."""

    user: str = "x-user-name"
    roles: str = "x-user-roles"


GATE_HEADERS = GateHeaders()
#: The QSINT answer when the user header is missing or empty.
MISSING_USER = {"error": "Missing X-User-Name header"}
_REQUIRES_USER = "requires_user"


def parse_roles(raw: str | None) -> tuple[str, ...]:
    """Split on ``,``, trim, drop empties, keep order and case."""
    if not raw:
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


@dataclass(frozen=True, slots=True)
class Identity:
    """Who is calling. Immutable and request-scoped (never stored in module state)."""

    user: str
    roles: tuple[str, ...] = ()
    is_super: bool = False
    raw_roles_header: str | None = None

    @property
    def is_anonymous(self) -> bool:
        """No (or an empty) user header was sent."""
        return not self.user

    def has_role(self, role: str) -> bool:
        """`role` is one of the caller's roles (exact match)."""
        return role in self.roles

    @classmethod
    def system(cls) -> Identity:
        """Identity of background work: no request behind it, unrestricted."""
        return cls(user="", roles=(), is_super=True, raw_roles_header=None)


def resolve_identity(
    headers: Mapping[str, str],
    super_users: Collection[str] = (),
    *,
    gate: GateHeaders = GATE_HEADERS,
) -> Identity:
    """The identity of one request.

    `is_super` is an exact, case-sensitive match of the user header against `super_users`.
    """
    user = headers.get(gate.user) or ""
    raw_roles = headers.get(gate.roles)
    return Identity(
        user=user,
        roles=parse_roles(raw_roles),
        is_super=bool(user) and user in super_users,
        raw_roles_header=raw_roles,
    )


class HasIdentity(Protocol):
    """What `@with_user` reads from a call: the resolved caller."""

    @property
    def identity(self) -> Identity:
        """The caller of the request."""
        ...


CallT = TypeVar("CallT", bound=HasIdentity)
Operation = Callable[[CallT], Awaitable[Any]]


def user_required(
    body: Any = MISSING_USER, status: int = 400
) -> Callable[[Operation[CallT]], Operation[CallT]]:
    """A decorator answering ``(body, status)`` instead of running the operation for an anonymous caller.

    Put it under `@api_operation`: the request chain validates the body before it invokes the
    operation, so a body error still wins over a missing user, as in flask-restx.
    """

    def decorate(handler: Operation[CallT]) -> Operation[CallT]:
        @functools.wraps(handler)
        async def guarded(call: CallT) -> Any:
            if call.identity.is_anonymous:
                return body, status
            return await handler(call)

        setattr(guarded, _REQUIRES_USER, True)
        return guarded

    return decorate


#: The QSINT services' ``@with_user``: 400 ``{"error": "Missing X-User-Name header"}``.
with_user = user_required()


def requires_user(handler: object) -> bool:
    """Whether `handler` (bare or wrapped by `@api_operation`) needs a named caller."""
    target = getattr(handler, "__operation_handler__", handler)
    return bool(getattr(target, _REQUIRES_USER, False))
