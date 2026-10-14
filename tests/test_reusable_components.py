"""Event logging, span helpers, circuit breaker, versioned cache, start-up gate and readiness."""

import asyncio
import json
import logging
import os
import time
import uuid

import fakeredis.aioredis
import httpx
import pytest
from fastapi import FastAPI

from framework.commons.events import (
    EventLogger,
    ScrubbingJSONFormatter,
    configure_event_logging,
    get_event_logger,
    redact,
    scrub,
)
from framework.health import (
    DEGRADED,
    NOT_READY,
    READY,
    DependenciesUnavailable,
    GatePolicy,
    install_readiness_routes,
    readiness_status,
    run_probes,
    wait_for_dependencies,
)
from framework.redis.versioned_cache import VersionedJsonCache
from framework.resilience import CircuitBreaker
from framework.testing.spans import capture_spans
from framework.tracing.spans import (
    current_trace_id,
    inject_http_headers,
    set_span_attributes,
    span_attributes,
    traced,
    traced_span,
)


# ------------------------------------------------------------------ event logging
def test_redact_hides_keys_long_strings_and_secrets():
    value = {
        "payload": {"a": 1},
        "id": "x" * 300,
        "dsn": "postgresql://user:hunter2@db:5432/x",
        "url": "https://s3/b?X-Amz-Signature=abc&x=1",
        "nested": [{"token": "t"}] * 25,
    }
    safe = redact(value)
    assert safe["payload"] == "<redacted len=1>"
    assert safe["id"] == "<truncated len=300>"
    assert safe["dsn"] == "postgresql://user:***@db:5432/x"
    assert safe["url"] == "https://s3/b?X-Amz-Signature=***&x=1"
    assert safe["nested"][0] == {"token": "<redacted len=1>"}
    assert safe["nested"][-1] == "<+5 more>"
    assert redact({"a": {"b": {"c": {"d": {"e": 1}}}}}) == {"a": {"b": {"c": {"d": "<nested>"}}}}
    assert redact({"secret_ish": 1}, keys={"secret_ish"}) == {"secret_ish": "<redacted len=1>"}


def test_scrub_leaves_plain_text_alone():
    assert scrub("plain words") == "plain words"
    assert scrub("redis://:pw@host and ?token=abc") == "redis://:***@host and ?token=***"


def test_event_logger_writes_fields_as_json(caplog):
    log = get_event_logger("tests.events").bind(request="r1")
    with caplog.at_level(logging.INFO, "tests.events"):
        log.info("order_accepted", order_id=7, payload="secret")
    record = caplog.records[-1]
    assert record.getMessage() == "order_accepted"
    assert record.event == {
        "event": "order_accepted",
        "request": "r1",
        "order_id": 7,
        "payload": "<redacted len=6>",
    }
    line = json.loads(ScrubbingJSONFormatter().format(record))
    assert line["order_id"] == 7 and line["event"] == "order_accepted"


def test_scrubbing_formatter_scrubs_tracebacks():
    try:
        raise ConnectionError("cannot reach postgresql://app:hunter2@db/x")
    except ConnectionError as exc:
        record = logging.LogRecord(
            "t", logging.ERROR, __file__, 1, "failed", (), (type(exc), exc, None)
        )
    line = json.loads(ScrubbingJSONFormatter().format(record))
    assert "hunter2" not in line["exception"] and "app:***@db" in line["exception"]


def test_scrub_hides_a_password_without_a_user():
    assert scrub("redis://:pw@h:6379/0") == "redis://:***@h:6379/0"


def _exception_record(exc: BaseException) -> logging.LogRecord:
    return logging.LogRecord(
        "t", logging.ERROR, __file__, 1, "failed", (), (type(exc), exc, exc.__traceback__)
    )


def test_scrubbing_formatter_cuts_long_traceback_lines():
    document = "Guvernul a anuntat masuri noi pentru economie. " * 40
    try:
        raise ValueError(f"cannot index {document} via redis://:pw@h:6379/0")
    except ValueError as exc:
        record = _exception_record(exc)
    rendered = json.loads(ScrubbingJSONFormatter().format(record))["exception"]
    assert document not in rendered and "pw" not in rendered
    assert "<truncated len=" in rendered and "Traceback" in rendered


def test_scrubbing_formatter_line_limit_is_configurable():
    try:
        raise ValueError("x" * 50)
    except ValueError as exc:
        record = _exception_record(exc)
    short = json.loads(ScrubbingJSONFormatter(max_line_length=20).format(record))["exception"]
    full = json.loads(ScrubbingJSONFormatter(max_line_length=100).format(record))["exception"]
    assert "<truncated len=62>" in short and "x" * 50 in full


def test_configure_event_logging_is_idempotent():
    configure_event_logging("tests_root", "DEBUG", quiet=("tests_noisy",))
    configure_event_logging("tests_root", "INFO", quiet=("tests_noisy",))
    root = logging.getLogger("tests_root")
    assert len([h for h in root.handlers if getattr(h, "_qf_events", False)]) == 1
    assert root.level == logging.INFO and logging.getLogger("tests_noisy").level == logging.WARNING
    assert isinstance(EventLogger(root), logging.LoggerAdapter)


# ------------------------------------------------------------------ span helpers
async def test_traced_spans_nest_redact_and_record_errors():
    @traced("orders.save", **{"order.kind": "web"})
    async def save():
        set_span_attributes(prompt="never logged", count=3)
        with traced_span("orders.child"):
            return current_trace_id()

    @traced()
    def fails():
        raise ValueError("bad")

    with capture_spans() as spans:
        trace_id = await save()
        with pytest.raises(ValueError):
            fails()
        parent, child = spans.one("orders.save"), spans.one("orders.child")
        failed = spans.one("test_reusable_components.fails")
    assert trace_id == f"{parent.context.trace_id:032x}"
    assert spans.children_of(parent) == [child]
    assert parent.attributes["prompt"] == "<redacted len=12>" and parent.attributes["count"] == 3
    assert failed.status.status_code.name == "ERROR"
    assert current_trace_id() is None


def test_span_attributes_are_opentelemetry_types():
    assert span_attributes({"a": None, "b": [1, 2], "c": {"x": 1}, "d": ("x", {})}) == {
        "b": [1, 2],
        "c": "{'x': 1}",
        "d": "['x', {}]",
    }


def test_inject_http_headers_carries_the_current_trace():
    with capture_spans():
        with traced_span("outgoing"):
            headers = inject_http_headers({"Accept": "application/json"})
    assert headers["Accept"] == "application/json" and headers["traceparent"].startswith("00-")


# ------------------------------------------------------------------ circuit breaker
def test_circuit_breaker_opens_probes_once_and_closes():
    clock = [0.0]
    breaker = CircuitBreaker(2, 10.0, clock=lambda: clock[0])
    breaker.failure()
    assert breaker.allow()
    breaker.failure()
    assert breaker.state == "open" and not breaker.allow()
    clock[0] = 10.0
    assert breaker.allow() and not breaker.allow()  # one half-open probe
    breaker.failure()  # a failed probe re-opens
    assert breaker.state == "open"
    clock[0] = 20.0
    assert breaker.allow()
    breaker.success()
    assert breaker.state == "closed"
    with pytest.raises(ValueError):
        CircuitBreaker(0)


# ------------------------------------------------------------------ versioned cache
def key(name, version):
    return f"t:cfg:v{version}:{name}"


class Counter:
    def __init__(self):
        self.loads = 0

    async def __call__(self):
        self.loads += 1
        return {"n": self.loads}


async def test_versioned_cache_round_trip_and_corrupt_value():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    calls = []
    cache = VersionedJsonCache(
        redis, key=key, observe=lambda op, outcome, _: calls.append((op, outcome))
    )
    loader = Counter()
    assert await cache.get_or_build("fields", 3, 60, loader) == {"n": 1}
    assert await cache.get_or_build("fields", 3, 60, loader) == {"n": 1}
    assert await redis.get("t:cfg:v3:fields") == '{"n": 1}'
    assert await cache.get_or_build("fields", 0, 60, loader) == {"n": 2}  # unknown: never cached

    def strict(raw):
        raise KeyError("shape")

    assert await cache.get_or_build("fields", 3, 60, loader, decode=strict) == {"n": 3}
    assert calls[:3] == [("get", "miss"), ("set", "ok"), ("get", "hit")]


async def test_versioned_cache_memo_skips_redis_while_the_version_is_current():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    cache = VersionedJsonCache(redis, key=key, memo=True)
    loader, decodes = Counter(), []
    await redis.set("t:cfg:v3:fields", '{"n": 0}')
    first = await cache.get_or_build(
        "fields", 3, 60, loader, decode=lambda raw: decodes.append(1) or raw
    )
    await redis.delete("t:cfg:v3:fields")
    for _ in range(5):
        assert await cache.get_or_build("fields", 3, 60, loader) is first
    assert (loader.loads, len(decodes)) == (0, 1)
    assert await cache.get_or_build("fields", 4, 60, loader) == {"n": 1}


async def test_a_frozen_redis_costs_one_budget_per_call_then_nothing():
    calls = 0

    class Frozen:
        async def get(self, key):
            nonlocal calls
            calls += 1
            await asyncio.sleep(3600)

        async def set(self, key, value, ex=None):
            await self.get(key)

    breaker = CircuitBreaker(3, 30.0)
    cache = VersionedJsonCache(Frozen(), key=key, breaker=breaker, budget_s=0.05)
    loader = Counter()
    started = time.monotonic()
    for version in range(1, 5):
        assert await cache.get_or_build("f", version, 60, loader) == {"n": version}
    assert calls == 3 and breaker.state == "open"
    assert time.monotonic() - started < 1.0


@pytest.mark.skipif(
    os.getenv("QF_INTEGRATION") != "1", reason="Set QF_INTEGRATION=1 for live Redis"
)
async def test_versioned_cache_on_a_live_redis():
    from redis.asyncio import Redis

    client = Redis.from_url(
        os.getenv("QF_TEST_REDIS", "redis://localhost:26379/0"), decode_responses=True
    )
    prefix = f"qf-test-{uuid.uuid4().hex[:8]}"
    cache = VersionedJsonCache(client, key=lambda name, version: f"{prefix}:{name}:{version}")
    try:
        loader = Counter()
        assert await cache.get_or_build("fields", 1, 60, loader) == {"n": 1}
        assert await cache.get_or_build("fields", 1, 60, loader) == {"n": 1}
        assert 0 < await client.ttl(f"{prefix}:fields:1") <= 60
        assert loader.loads == 1
    finally:
        await client.delete(f"{prefix}:fields:1")
        await client.aclose()


# ------------------------------------------------------------------ start-up gate
class Flaky:
    def __init__(self, failures):
        self.failures, self.calls = failures, 0

    async def __call__(self):
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError("refused")


async def test_gate_waits_with_backoff_until_every_dependency_answers(caplog):
    sleeps = []

    async def sleep(delay):
        sleeps.append(delay)

    pg, kafka = Flaky(0), Flaky(5)
    with caplog.at_level(logging.INFO, "framework.health"):
        await wait_for_dependencies(
            {"postgres": pg, "kafka": kafka},
            required=("postgres", "kafka", "s3"),
            timeout_s=0,
            sleep=sleep,
            policy=GatePolicy(backoff_start_s=1.0, backoff_cap_s=4.0),
        )
    assert (pg.calls, kafka.calls) == (1, 6)
    assert sleeps == [1.0, 2.0, 4.0, 4.0, 4.0]
    messages = [r.getMessage() for r in caplog.records]
    assert messages.count("dependency_unavailable") == 5
    assert "dependency_check_not_applicable" in messages and "dependencies_ready" in messages


async def test_gate_gives_up_after_the_deadline():
    async def sleep(delay):
        return None

    with pytest.raises(DependenciesUnavailable, match="redis"):
        await wait_for_dependencies(
            {"redis": Flaky(10**6)}, required=("redis",), timeout_s=0.0001, sleep=sleep
        )


async def test_gate_times_out_a_hanging_check():
    async def hangs():
        await asyncio.sleep(3600)

    async def sleep(delay):
        return None

    with pytest.raises(DependenciesUnavailable):
        await wait_for_dependencies(
            {"kafka": hangs},
            required=("kafka",),
            timeout_s=0.5,
            sleep=sleep,
            policy=GatePolicy(check_timeout_s=0.05, backoff_start_s=1.0),
        )


async def test_gate_waits_only_for_the_required_dependencies():
    never, ok = Flaky(10**6), Flaky(0)
    await wait_for_dependencies(
        {"kafka": never, "postgres": ok}, required=("postgres",), timeout_s=1
    )
    assert never.calls == 0 and ok.calls == 1


# ------------------------------------------------------------------ readiness
async def test_run_probes_treats_errors_and_timeouts_as_unhealthy():
    async def ok():
        return True

    async def boom():
        raise ConnectionError()

    async def slow():
        await asyncio.sleep(3600)

    results = await run_probes({"a": ok(), "b": boom(), "c": slow()}, timeout_s=0.05)
    assert results == {"a": True, "b": False, "c": False}


@pytest.mark.parametrize(
    "checks, started, expected",
    [
        ({"pg": {"ok": True}, "redis": {"ok": True}}, True, READY),
        ({"pg": {"ok": True}, "redis": {"ok": False}}, True, DEGRADED),
        ({"pg": {"ok": False}, "redis": {"ok": True}}, True, NOT_READY),
        ({"redis": {"ok": True}}, True, NOT_READY),
        ({"pg": {"ok": True}}, False, NOT_READY),
    ],
)
def test_readiness_status(checks, started, expected):
    assert readiness_status(checks, required={"pg"}, started=started) == expected


async def test_readiness_routes_replace_the_framework_probe():
    from framework.api.server import create_app
    from framework.config import FrameworkSettings

    app = create_app(FrameworkSettings(enable_etl=False, enable_dynamic_endpoints=False))
    state = {"status": READY}

    async def report(application: FastAPI):
        return {"status": state["status"], "checks": {}}

    install_readiness_routes(app, report)
    assert sum(getattr(r, "path", None) == "/health/ready" for r in app.router.routes) == 1
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t"
    ) as client:
        assert (await client.get("/health/ready")).status_code == 200
        state["status"] = DEGRADED
        assert (await client.get("/health/dependencies")).status_code == 200
        state["status"] = NOT_READY
        resp = await client.get("/health/ready")
    assert (resp.status_code, resp.json()) == (503, {"status": "not_ready", "checks": {}})
