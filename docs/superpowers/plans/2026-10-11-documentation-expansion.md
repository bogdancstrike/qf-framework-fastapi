# Documentation and runnable pipeline expansion

**Goal:** Let a developer run a complete HTTP/Kafka/Redis pipeline, understand its delivery contract, and diagnose it without reading framework internals.

**Scope:** Add a documentation index, a hands-on tutorial, a worker contract/cookbook, testing guidance and troubleshooting. Add an isolated example package with HTTP ingress, fanout, two branches, Redis join and a CLI that provisions topics and checks final results. Preserve the existing example and measured benchmark artifacts.

- [x] Add `examples/pipeline/{topics,workers,app,client}.py` and its README. The CLI must use unique job IDs, bounded waits and real output validation; application startup/shutdown owns its ingress client.
- [x] Add `docs/README.md`, `GettingStarted.md`, `Workers.md`, `Testing.md` and `Troubleshooting.md`. Cross-link existing configuration, operations, tracing and comparison references. State ordering, replay, join retention and trace boundaries precisely.
- [x] Add an opt-in live test that runs the documented CLI and Uvicorn application against isolated Kafka topics and Redis keys, then deletes only those topics.
- [x] Run formatting, unit tests and the new live walkthrough. Check Markdown links and syntax; update README and task status; commit and push. Existing performance measurements remain unchanged; collector proof is refreshed by the live suite.
