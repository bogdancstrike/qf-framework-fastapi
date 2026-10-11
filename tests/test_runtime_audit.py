"""Regression scenarios for concurrent policy state and extension boundaries."""

import asyncio
import json
import sys
from dataclasses import replace
from types import SimpleNamespace

import fakeredis.aioredis
import pytest

from framework.api.dynamic import generate_endpoints_from_config
from framework.api.server import create_app
from framework.app import FrameworkApp
from framework.config import FrameworkSettings, load_framework_settings
from framework.decorators.intern import CircuitBreakerConfig, RateLimitConfig
from framework.decorators.kafka_workers import validate_workers
from framework.etl.framework_etl import KafkaRuntime
from framework.etl.processing import Processor
from framework.observability import Metrics
from framework.redis.aggregation import AggregationStore
from conftest import Consumer, Producer, eventually, record


async def test_old_job_success_cannot_close_newly_opened_worker_circuit(
    settings, spec, fake_module
):
    entered, release = asyncio.Event(), asyncio.Event()

    async def work(message, *_):
        if message["id"] == "old":
            entered.set()
            await release.wait()
            return message
        raise ValueError("open the circuit")

    settings.retry_attempts = 1
    consumer = Consumer([record(0, b'{"id":"old"}'), record(1, b'{"id":"fail"}')])
    runtime = KafkaRuntime(
        settings,
        consumer=consumer,
        producer=Producer(),
        workers=[replace(spec, fn=work, circuit_breaker=CircuitBreakerConfig(1, 30))],
    )
    await runtime.start()
    try:
        await entered.wait()
        policy = runtime.admission[spec.name]
        await eventually(lambda: policy.open_until > 0)
        release.set()
        await eventually(lambda: consumer.commits.get(consumer.tp) == 2)
        assert policy.open_until > 0, "An older successful job must not erase a newer failure"
    finally:
        release.set()
        await runtime.stop()


async def test_cancelled_half_open_job_releases_probe(settings, spec, fake_module):
    entered = asyncio.Event()

    async def wait(message, *_):
        entered.set()
        await asyncio.Event().wait()

    runtime = KafkaRuntime(
        settings,
        consumer=Consumer(),
        producer=Producer(),
        workers=[replace(spec, fn=wait, circuit_breaker=CircuitBreakerConfig(1, 30))],
    )
    await runtime.start()
    try:
        policy = runtime.admission[spec.name]
        policy.open_until = 0.0001  # A previously opened circuit whose delay has elapsed.
        runtime.enqueue(runtime.consumer.tp, [record()])
        runtime.tick()
        await entered.wait()
        assert policy.probe
        await runtime.revoke({runtime.consumer.tp})
        assert not policy.probe, "Cancellation must not block all future half-open probes"
    finally:
        await runtime.stop()


async def test_probe_cancelled_before_first_task_step_releases_admission(
    settings, spec, fake_module
):
    runtime = KafkaRuntime(
        settings,
        consumer=Consumer(),
        producer=Producer(),
        workers=[replace(spec, circuit_breaker=CircuitBreakerConfig(1, 30))],
    )
    await runtime.start()
    try:
        policy = runtime.admission[spec.name]
        policy.open_until = 0.0001
        runtime.enqueue(runtime.consumer.tp, [record()])
        runtime.tick()
        task = next(iter(runtime.active[spec.name]))
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert not policy.probe
        assert not runtime.producer.sent
    finally:
        await runtime.stop()


@pytest.mark.parametrize(
    "change",
    [
        {"kind": "unknown"},
        {"fn": None},
        {"max_workers": 1.5},
        {"topics_out": ["out", "out"]},
        {"rate_limit": RateLimitConfig(float("nan"), 1)},
        {"rate_limit": RateLimitConfig(float("inf"), 1)},
    ],
)
def test_invalid_injected_worker_contracts_fail_fast(spec, change):
    with pytest.raises(ValueError):
        validate_workers([replace(spec, **change)])


async def test_running_worker_configuration_is_a_snapshot(settings, spec, fake_module):
    worker = replace(spec, metadatas={"nested": {"value": "original"}})
    runtime = KafkaRuntime(settings, consumer=Consumer(), producer=Producer(), workers=[worker])
    await runtime.start()
    try:
        worker.topics_in.append("mutated")
        worker.metadatas["nested"]["value"] = "mutated"
        assert runtime.workers[0].topics_in == ["in"]
        assert runtime.workers[0].metadatas == {"nested": {"value": "original"}}
    finally:
        await runtime.stop()


async def test_lost_processing_lease_fails_runtime_instead_of_stalling_partition(
    settings, spec, fake_module
):
    settings.idempotency_enabled = True
    settings.aggregate_lease_sec = 3
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = AggregationStore(settings, redis)
    entered, release = asyncio.Event(), asyncio.Event()

    async def work(message, *_):
        entered.set()
        await release.wait()
        return message

    consumer, producer = Consumer([record()]), Producer()
    runtime = KafkaRuntime(
        settings,
        consumer=consumer,
        producer=producer,
        aggregation=store,
        workers=[replace(spec, fn=work)],
    )
    await runtime.start()
    try:
        await entered.wait()
        ledger = replace(spec, name="dedup:" + spec.name)
        keys = store.keys(ledger, "source:in:0:0")
        assert await redis.delete(keys[1]) == 1
        await asyncio.sleep(1.15)  # Heartbeat detects lost ownership.
        release.set()
        await eventually(lambda: not runtime.active[spec.name])
        assert runtime.error is not None, "An owned partition must not silently retain a failed gap"
        assert not runtime.ready
        assert not producer.sent and not consumer.commits
    finally:
        release.set()
        await runtime.stop()
        await redis.aclose()


async def test_ready_join_parts_live_at_least_as_long_as_renewed_lease(settings, spec):
    settings.aggregate_lease_sec = 3
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = AggregationStore(settings, redis)
    worker = replace(spec, topics_in=["a", "b"], aggregator_timeout_sec=1)
    try:
        await store.claim(worker, "x", "a", {"a": 1})
        claim = await store.claim(worker, "x", "b", {"b": 2})
        assert await redis.ttl(claim.keys[0]) >= 2
        async with store.heartbeat(claim):
            await asyncio.sleep(1.15)
            assert await redis.exists(claim.keys[0])
            retried = await store.claim(worker, "x", "b", {"b": 2}, token=claim.token)
            assert retried.payload == {"a": 1, "b": 2}
    finally:
        await redis.aclose()


async def test_join_handler_failure_is_reported_after_acknowledged_dlq(settings, spec):
    async def fail(*args):
        raise ValueError("business failure")

    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    producer = Producer()
    settings.retry_attempts = 1
    store = AggregationStore(settings, redis)
    processor = Processor(settings, producer, Metrics(), aggregation=store)
    worker = replace(spec, kind="aggregator", topics_in=["a", "b"], aggregate_by="id", fn=fail)
    try:
        await processor.handle(worker, [record(topic="a")], lambda: True)
        outcome = await processor.handle(worker, [record(topic="b")], lambda: True)
        assert outcome is False
        assert [topic for topic, *_ in producer.sent] == [settings.error_topic]
        assert (await store.claim(worker, "x", "a", {"id": "x"})).status == "done"
    finally:
        await redis.aclose()


@pytest.mark.parametrize("duplicate", ["name", "topic"])
async def test_explicit_worker_specs_reject_collisions_before_connecting(
    settings, spec, fake_module, duplicate
):
    other = replace(
        spec,
        name=spec.name if duplicate == "name" else "other",
        topics_in=["other"] if duplicate == "name" else spec.topics_in,
    )
    runtime = KafkaRuntime(
        settings, consumer=Consumer(), producer=Producer(), workers=[spec, other]
    )
    with pytest.raises((ValueError, RuntimeError), match="[Dd]uplicate|already handled"):
        await runtime.start()


@pytest.mark.parametrize("field", ["shutdown_timeout_sec", "retry_base_sec", "redis_timeout_sec"])
def test_nonfinite_settings_are_rejected(field):
    with pytest.raises(ValueError):
        FrameworkSettings(enable_etl=False, **{field: float("inf")})


def test_process_environment_beats_dotenv_across_aliases(tmp_path, monkeypatch):
    dotenv = tmp_path / ".env"
    dotenv.write_text("QSINT_CONSUMER_NAME=from-file\nQSINT_ENABLE_ETL=false\n")
    monkeypatch.delenv("QSINT_CONSUMER_NAME", raising=False)
    monkeypatch.setenv("WORKER_NAME", "from-process")
    loaded = load_framework_settings(dotenv_path=dotenv)
    assert loaded.consumer_name == "from-process"


@pytest.mark.parametrize("models", [[], False, 0, ""])
def test_invalid_falsy_model_containers_fail_construction(tmp_path, models):
    path = tmp_path / "endpoint.json"
    path.write_text(json.dumps({"namespaces": [], "models": models, "endpoints": []}))
    with pytest.raises(ValueError, match="models must"):
        generate_endpoints_from_config(create_app(), path)


def test_renamed_path_parameters_do_not_create_unreachable_routes(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "audit_api", SimpleNamespace(handler=lambda **kw: {}))
    path = tmp_path / "endpoint.json"
    path.write_text(
        json.dumps(
            {
                "namespaces": [{"name": "items"}],
                "endpoints": [
                    {
                        "namespace": "items",
                        "operation_name": name,
                        "api_url": uri,
                        "request_method": ["GET"],
                        "exec_method": {"module_name": "audit_api", "method_name": "handler"},
                    }
                    for name, uri in [("first", "/items/{id}"), ("second", "/items/{other}")]
                ],
            }
        )
    )
    app = create_app()
    original_count = len(app.routes)
    with pytest.raises(ValueError, match="Duplicate route"):
        generate_endpoints_from_config(app, path)
    assert len(app.routes) == original_count


async def test_lifespan_flushes_tracing_even_when_runtime_cleanup_fails(monkeypatch):
    from framework.app import runner

    flushed = []
    monkeypatch.setattr(runner, "shutdown_tracing", lambda: flushed.append(True))

    class Runtime:
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            pass

        async def stop(self):
            raise RuntimeError("cleanup failed")

    app = FrameworkApp(
        FrameworkSettings(
            worker_modules=["x"],
            kafka_bootstrap_servers="x",
            consumer_name="x",
            enable_dynamic_endpoints=False,
        ),
        runtime_factory=Runtime,
    ).build_app()
    with pytest.raises(RuntimeError, match="cleanup failed"):
        async with app.router.lifespan_context(app):
            pass
    assert flushed == [True]


async def test_startup_error_survives_secondary_cleanup_error(monkeypatch):
    from framework.app import runner

    flushed = []
    monkeypatch.setattr(runner, "shutdown_tracing", lambda: flushed.append(True))

    class Runtime:
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            raise ValueError("primary startup failure")

        async def stop(self):
            raise RuntimeError("secondary cleanup failure")

    app = FrameworkApp(
        FrameworkSettings(
            worker_modules=["x"],
            kafka_bootstrap_servers="x",
            consumer_name="x",
            enable_dynamic_endpoints=False,
        ),
        runtime_factory=Runtime,
    ).build_app()
    with pytest.raises(ValueError, match="primary startup failure"):
        async with app.router.lifespan_context(app):
            pass
    assert flushed == [True]


async def test_worker_factory_failure_restores_signal_handlers(monkeypatch):
    from framework.app import runner

    loop = asyncio.get_running_loop()
    added, removed, flushed = [], [], []
    monkeypatch.setattr(loop, "add_signal_handler", lambda signum, callback: added.append(signum))
    monkeypatch.setattr(loop, "remove_signal_handler", lambda signum: removed.append(signum))
    monkeypatch.setattr(runner, "shutdown_tracing", lambda: flushed.append(True))

    def factory(*args, **kwargs):
        raise ValueError("factory failure")

    framework = FrameworkApp(FrameworkSettings(enable_etl=False), runtime_factory=factory)
    with pytest.raises(ValueError, match="factory failure"):
        await framework.run_worker()
    assert added == removed and len(removed) == 2
    assert flushed == [True]
