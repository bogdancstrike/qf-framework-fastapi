# Getting started

This guide takes you from installation to a verified multi-stage pipeline. Run shell commands from the repository root. Keep QF Flask and this distribution in separate virtual environments because both export the Python package `framework`.

## 1. Install and run HTTP without services

```bash
git clone https://github.com/bogdancstrike/qf-framework-fastapi.git
cd qf-framework-fastapi
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev,tracing]'
uvicorn examples.app:app --port 4000
```

In a second activated terminal:

```bash
curl http://localhost:4000/examples/ping
curl -X POST http://localhost:4000/examples/echo/42 \
  -H 'Content-Type: application/json' -d '{"text":"hello"}'
curl http://localhost:4000/health/ready
```

The echo returns ID `42` and your payload. The ping has `model_name: null` in [endpoint.json](../examples/maps/endpoint.json), so no input model/body is required. Visit `http://localhost:4000/docs` for OpenAPI. Stop this server before starting the next application on the same port.

## 2. Run a full pipeline

```bash
docker compose -p qf-fastapi-test up -d --wait
python -m examples.pipeline.client create-topics
uvicorn examples.pipeline.app:app --port 4000 --workers 1
```

Then:

```bash
python -m examples.pipeline.client run --jobs 3 --timeout 60
```

Expect three submissions, three merged results and a final `"verified": 3`. This traverses HTTP → Kafka → normalization/fanout → two branch workers → Redis join → Kafka output. See the [complete walkthrough](../examples/pipeline/README.md) for exact payloads, isolation, error injection and Jaeger configuration.

The compose stack exposes Kafka on `29092`, Redis on `26379`, Jaeger OTLP gRPC on `24317` and its UI on `26686`. Those host addresses differ from compose-network names (`kafka:9092`, `redis:6379`, `jaeger:4317`).

## 3. Create your application package

Use this structure:

```text
myapp/
  __init__.py
  main.py
  workers.py
```

Save `myapp/workers.py`:

```python
from framework.decorators import kafka_handler, retry_to_dlq


@retry_to_dlq(max_attempts=3, dlq_topic="orders.dlq")
@kafka_handler(
    name="validate-order",
    topics_in=["orders.in"],
    topics_out=["orders.validated"],
    max_workers=8,
)
async def validate_order(message, consumer_name, metadatas):
    if message["quantity"] <= 0:
        raise ValueError("quantity must be positive")
    return {**message, "validated": True}
```

Save `myapp/main.py`:

```python
from framework.app import FrameworkApp
from framework.config import FrameworkSettings

settings = FrameworkSettings(
    enable_dynamic_endpoints=False,
    worker_modules=["myapp.workers"],
    kafka_bootstrap_servers="localhost:29092",
    consumer_name="orders-v1",
    error_topic="orders.dlq",
)
framework = FrameworkApp(settings)
app = framework.build_app()

if __name__ == "__main__":
    framework.serve()
```

Provision `orders.in`, `orders.validated` and `orders.dlq` through your Kafka tooling. Start with `uvicorn myapp.main:app --port 4000 --workers 1`. Publish `{"id":"order-001","quantity":2}` to `orders.in` using the console producer pattern in the [repository quickstart](../README.md#enable-the-bundled-kafka-workers). This plain worker needs no Redis.

The constructor values above are explicit overrides. To make deployment values configurable, leave those arguments out and supply `QSINT_WORKER_MODULES='["myapp.workers"]'`, `QSINT_KAFKA_BOOTSTRAP_SERVERS`, `QSINT_CONSUMER_NAME` and the other [environment settings](Configuration.md). Set `QSINT_ENABLE_API=false` and run `python -m myapp.main` for worker-only serving. Worker-only mode has no HTTP health/metrics server.

## 4. Verify behavior before tuning

Confirm output content first, then broker-reported source commits, retry/DLQ behavior and readiness after a dependency failure. Add application tests around your own transformations and side effects. The [testing guide](Testing.md) provides concrete commands and assertions.

Keep business IDs stable across stages. Add business-level idempotency where a repeated operation would be harmful. Increasing concurrency before understanding downstream limits can increase retries, queueing and replay volume. The [worker cookbook](Workers.md) explains each decision, and [operations](Operations.md) documents recovery and tuning.
