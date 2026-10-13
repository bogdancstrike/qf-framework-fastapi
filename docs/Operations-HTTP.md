# Legacy HTTP contract for endpoint-map routes (2.2)

`framework.operations` serves endpoint-map routes with the answers a Flask / flask-restx /
Werkzeug service gives, byte for byte. Use it when a service moves to the framework but its
clients (UIs, other services, gateways) must not notice: same validation messages, same error
bodies, same 404 page, same JSON bytes.

Install the extra: `pip install "qf-fastapi[operations]"` (adds `jsonschema`).

## What a request goes through

| Step | Where | Legacy answer |
|---|---|---|
| Unknown path, any method | exception handlers | Werkzeug's HTML 404 |
| Known path, wrong method | exception handlers | JSON 405 with `Allow`; `OPTIONS` → empty 200, `text/html` |
| Body above the limit | `BodySizeLimitMiddleware` | 413 `{"message": "The data value transmitted exceeds the capacity limit."}` (415 first for non-JSON) |
| Non-JSON `Content-Type` | `MediaTypeGuard` | 415 Werkzeug message |
| Empty or malformed JSON | `JsonDecoder` | 400 Werkzeug message |
| Non-object body where an object is needed | `BodyKindPolicy` | 400 flask-restx validation body |
| Model violation | `SchemaValidator` | 400 `{"errors": {path: message}, "message": "Input payload validation failed"}` |
| Caller | `IdentityResolver` | never answers; `call.identity` |
| No user on a `@with_user` operation | the decorator | 400 `{"error": "Missing X-User-Name header"}` |
| The operation | `HandlerInvoker` (span `operation.<ns>.<op>`) | body → 200; `(body, status[, headers])`; or a `Response` |
| `OperationError(status, body)` | `ErrorMapper` | that exact body and status |
| `ClientInputError(path, reason)` | `ErrorMapper` → route `ApiProfile` | the profile's dialect |
| Anything else | `ErrorMapper` | 500 `{"message": "Internal Server Error"}`, traceback logged, nothing echoed |

Every JSON answer is encoded like Flask's `jsonify` (`LegacyJSONResponse`: ASCII escapes,
trailing newline). `SecurityHeadersMiddleware` adds flask-talisman's defaults (HSTS only over
HTTPS); `EchoOriginCorsMiddleware` behaves like flask-cors; `TrailingSlashMiddleware` makes the
trailing slash optional without a redirect.

## Declaring routes

One map file per route group. It is an ordinary framework endpoint map plus two keys:

```json
{
  "group": "search",
  "namespaces": [{"name": "qdata", "description": "Search API"}],
  "operation_schemas": {
    "save": {
      "alias": {"type": "string", "args": {"required": true}},
      "is_public": {"type": "boolean", "args": {"required": true}},
      "platform": {"type": "string", "args": {"enum_ci": ["Facebook", "X"], "enum_ci_aliases": ["twitter"]}},
      "message": {"type": "nested", "model": "message"}
    },
    "message": {"system": {"type": "dict"}}
  },
  "endpoints": [{
    "namespace": "qdata",
    "operation_name": "save_search",
    "request_method": ["POST"],
    "exec_method": {"module_name": "myapp.searches.routes", "method_name": "save_search"},
    "operation_contract": {"model": "save", "body_kind": "validated"}
  }]
}
```

- Route models go under `operation_schemas`, **not** the framework's `models` (that key
  validates with Pydantic and answers 422; `load_map` refuses it).
- `body_kind`: `validated` (object checked against `model`), `object` (any object), `raw`
  (object; `[]` and `null` read as `{}`), `ignored` (any JSON, read as `{}`), `none` (GET).
- Field types: `string`, `integer`, `boolean`, `number`/`float`, `dict`/`object`/`raw`,
  `list` (with `super`), `nested` (with `model`, also as `list` items). `args`: `required`,
  `enum`, `enum_ci` (+ `enum_ci_aliases`). Types are strict, `null` is rejected, unknown keys
  pass, defaults are inert: what flask-restx validates.

## Writing operations

```python
from framework.operations import ClientInputError, OperationCall, OperationError, api_operation, with_user


@api_operation
@with_user
async def save_search(call: OperationCall) -> dict:
    if call.payload["alias"].startswith("_"):
        raise ClientInputError("alias", "reserved prefix")
    if not await store.can_write(call.identity):
        raise OperationError(403, {"message": "Forbidden"})
    return {"owner": call.identity.user}
```

`call.body` is the decoded body, `call.payload` the body as a dict, `call.identity` the caller,
`call.ctx` whatever the application put in `app.state.ctx`, `call.spec` the route contract.

`@with_user` must be **under** `@api_operation`. The chain validates the body before it runs
the operation, so a body error wins over a missing user, as in flask-restx. `user_required(body,
status)` builds a decorator with another answer. `requires_user(handler)` tells whether an
operation is guarded (the catalogue exposes it as `OperationSpec.with_user`).

## Configuring the chain

```python
from framework.operations import (
    ErrorFieldProfile, ErrorMapper, GateHeaders, OperationChain, OperationObserver,
    ProfileRegistry, default_links, install_operations, load_map,
)

mapper = ErrorMapper().register(               # most specific class wins
    AccessDenied, lambda exc, spec: ({"status": "FORBIDDEN", "roles": exc.roles}, 403)
)


class Metrics(OperationObserver):
    def on_response(self, spec, status, duration_s):
        responses.labels(spec.group, spec.path, str(status)).inc()


chain = OperationChain(
    default_links(
        super_users=lambda ctx: ctx.settings.super_users,   # exact, case-sensitive match
        gate=GateHeaders(user="x-user-name", roles="x-user-roles"),
        span_prefix="operation",
    ),
    mapper,
    Metrics(),
)
catalogue = install_operations(
    app,
    [load_map(path) for path in map_paths],
    chain=chain,
    profiles=ProfileRegistry({"qdata": ErrorFieldProfile("error", "Invalid request: {path}")}),
    max_body_bytes=16 << 20,
    cors_origins=settings.cors_allowed_origins,
    extra_middleware=[(HttpMetricsMiddleware, {"owner": app})],
)
app.state.ctx = context
```

| Extension point | Contract |
|---|---|
| `ApiProfile` | `client_input_error(path, reason) -> (body, status)`; `non_object_body(body) -> (body, status) | None` answers a non-object JSON body where the route needs an object (`None`: the flask-restx validation body; override it for a family whose legacy service answered e.g. Werkzeug's 400 for `null`); registered per namespace in `ProfileRegistry`, or named by a map (`"api_profile": "module:Class"`). Default: the flask-restx validation body. `ProfileRegistry(default=None)` makes an unknown namespace a start-up error |
| `ErrorMapper.register(exc_type, answer)` | `answer(exc, spec) -> (body, status)`; checked before the built-in types, along the exception's MRO |
| `OperationObserver.on_response(spec, status, duration_s)` | Called once per answered request; the default logs `operation_request` (DEBUG) / `operation_request_failed` (ERROR) |
| `ChainLink.handle(state) -> Response | None` | Insert your own link in the tuple you pass to `OperationChain`; `state.extras` is free for it |
| `extra_middleware` | Added inside the trailing-slash middleware, so they see the matched route template |

Middleware order, outermost first: security headers, CORS echo, trailing slash, your extra
middleware, body size limit, then the framework's observability middleware and routing.

`describe_routes(catalogue, mounted_groups)` gives one line per route for a CLI listing.
