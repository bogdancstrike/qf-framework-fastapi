"""Operation layer: endpoint-map routes served with the Flask / flask-restx / Werkzeug HTTP contract.

For services migrating from Flask that must keep their wire contract byte for byte: request
validation answers, error bodies, encoding, 404/405 pages, size limit, security headers and
CORS behave like the Flask service they replace. Needs the ``operations`` extra (``jsonschema``).
See ``docs/Operations-HTTP.md``.
"""

from .binding import (
    APP_STATE_CATALOGUE,
    APP_STATE_CHAIN,
    APP_STATE_CONTEXT,
    api_operation,
    install_operations,
    mount_endpoint_maps,
)
from .catalogue import (
    CatalogueError,
    EndpointMap,
    OperationSpec,
    RouteCatalogue,
    describe_routes,
    load_map,
)
from .chain import (
    ChainLink,
    ChainState,
    HandlerInvoker,
    IdentityResolver,
    OperationCall,
    OperationChain,
    OperationObserver,
    default_links,
)
from .encoding import LegacyJSONResponse, respond
from .errors import ClientInputError, ErrorMapper, OperationError
from .identity import (
    GateHeaders,
    Identity,
    parse_roles,
    requires_user,
    resolve_identity,
    user_required,
    with_user,
)
from .profiles import ApiProfile, ErrorFieldProfile, ProfileRegistry, ValidationBodyProfile

__all__ = [
    "APP_STATE_CATALOGUE",
    "APP_STATE_CHAIN",
    "APP_STATE_CONTEXT",
    "ApiProfile",
    "CatalogueError",
    "ChainLink",
    "ChainState",
    "ClientInputError",
    "EndpointMap",
    "ErrorFieldProfile",
    "ErrorMapper",
    "GateHeaders",
    "HandlerInvoker",
    "Identity",
    "IdentityResolver",
    "LegacyJSONResponse",
    "OperationCall",
    "OperationChain",
    "OperationError",
    "OperationObserver",
    "OperationSpec",
    "ProfileRegistry",
    "RouteCatalogue",
    "ValidationBodyProfile",
    "api_operation",
    "default_links",
    "describe_routes",
    "install_operations",
    "load_map",
    "mount_endpoint_maps",
    "parse_roles",
    "requires_user",
    "resolve_identity",
    "respond",
    "user_required",
    "with_user",
]
