from fastapi import Request
from framework.tracing import get_tracer


async def echo(app, request: Request, operation, item_id: int):
    with get_tracer().start_as_current_span("example.echo"):
        return {"id": item_id, "operation": operation, "payload": await request.json()}


def legacy_echo(app, request, operation):
    return {"operation": operation, "payload": request.get_json()}, 201
