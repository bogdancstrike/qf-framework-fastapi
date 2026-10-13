"""The exact answers Werkzeug and Flask give, which legacy clients already handle.

Kept in one place so every part of the operation layer (chain, exception handlers,
middleware) answers with the same bytes.
"""

from __future__ import annotations

#: Empty or malformed JSON body.
BAD_REQUEST = {
    "message": "The browser (or proxy) sent a request that this server could not understand."
}
#: Body sent with a non-JSON ``Content-Type``.
UNSUPPORTED_MEDIA = {
    "message": "Did not attempt to load JSON data because the request Content-Type was not "
    "'application/json'."
}
#: Any unhandled error (no traceback, no request data).
INTERNAL_ERROR = {"message": "Internal Server Error"}
#: Known path, wrong method.
METHOD_NOT_ALLOWED = {"message": "The method is not allowed for the requested URL."}
#: Body above the size limit (``MAX_CONTENT_LENGTH``).
TOO_LARGE = {"message": "The data value transmitted exceeds the capacity limit."}
#: Unknown path, any method: Werkzeug's HTML page.
NOT_FOUND_HTML = (
    "<!doctype html>\n<html lang=en>\n<title>404 Not Found</title>\n<h1>Not Found</h1>\n"
    "<p>The requested URL was not found on the server. If you entered the URL manually "
    "please check your spelling and try again.</p>\n"
)
#: flask-restx validation error message.
VALIDATION_MESSAGE = "Input payload validation failed"


def is_json_mimetype(content_type: str | None) -> bool:
    """Werkzeug's ``is_json``: ``application/json`` or ``application/*+json``, parameters ignored."""
    mimetype = (content_type or "").split(";", 1)[0].strip().lower()
    return mimetype == "application/json" or (
        mimetype.startswith("application/") and mimetype.endswith("+json")
    )
