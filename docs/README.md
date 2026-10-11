# QF developer documentation

Start with the path matching what you need to do.

| Task | Guide |
|---|---|
| Run an API and your first complete worker pipeline | [Getting started](GettingStarted.md) |
| Understand worker arguments, returns, ordering, batches and joins | [Worker contracts and cookbook](Workers.md) |
| Run tests or reproduce a performance claim | [Testing and validation](Testing.md) |
| Diagnose stalled pipelines, duplicates or missing spans | [Troubleshooting](Troubleshooting.md) |
| Move an existing Flask/gevent application | [Migration](Migration.md) |
| Find an exact setting, default or legacy alias | [Configuration reference](Configuration.md) |
| Deploy, tune, monitor, replay DLQs or recover after failure | [Operations and recovery](Operations.md) |
| Scale replicas and recover killed processes | [Horizontal scaling and restart recovery](Scaling.md) |
| Use execution leases, common decorators and owned clients | [Utility audit and contracts](Utilities.md) |
| Review ACK/Redis hardening and measured before/after costs | [Reliability hardening](Hardening.md) |
| Choose optional future framework features | [Twenty feature proposals](Roadmap.md) |
| Follow trace IDs through Kafka, batches and Redis joins | [Distributed tracing](Tracing.md) |
| Explain measured costs and gains to management | [Flask versus FastAPI report](qf_flask_vs_qf_fastapi_report.md) |
| Extend or review the framework itself | [Architecture](architecture.md), [specification](specs.md), [task status](tasks.md) |

The [example catalog](../examples/README.md) distinguishes HTTP-only examples from Kafka/Redis applications. The [benchmark guide](../benchmarks/README.md) documents the matched old/new harnesses and raw artifacts. The [original analysis](Analysis.md) explains reproduced defects motivating the new delivery contract.

## The contracts to learn first

1. Workers return JSON-compatible dictionaries, lists of dictionaries, or `None` for successful filtering. Async handlers must avoid blocking the event loop.
2. Kafka output/DLQ acknowledgements precede source completion. Source commits cover a completed prefix of fetched work; execution and output can still finish out of order.
3. Processing is at least once. Producer idempotence and the optional Redis replay ledger have distinct scopes and finite recovery boundaries.
4. Joins correlate parts by application ID. Their timeout also bounds retention and completion suppression; they are not unbounded durable workflow storage.
5. The ASGI lifespan owns startup and shutdown. Importing an application does not start consumers. Each serving process owns its clients, runtime and metrics.

Examples target the checked-in implementation. Measured results include both 1,000 and 100,000-message bursts; always retain their delivery-mode and workload qualifications when quoting speedups.
