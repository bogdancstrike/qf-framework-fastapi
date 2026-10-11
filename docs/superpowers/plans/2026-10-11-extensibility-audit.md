# Runtime correctness and extensibility audit

The objective is to reproduce and fix remaining concurrency, lifecycle and
configuration bugs without changing the public worker/configuration conventions.
Historical benchmark results remain attributed to their measured revisions.

- [x] Reproduce stale circuit completions and cancelled half-open probes.
- [x] Verify aggregation retention during active leases and recovery.
- [x] Audit explicit worker registration, settings and endpoint validation.
- [x] Audit application startup/shutdown resource ownership and cleanup.
- [x] Add focused regression tests and document state-machine invariants.
- [x] Document extension points and failure contracts in docs and README.
- [x] Run unit, real-service, recovery and telemetry checks on Python 3.12/3.15.
- [x] Commit and push verified changes; record evidence and remaining limits.

Use small, composable changes. Prefer existing adapters, policies and constructor
injection to new abstractions without a concrete use case. Performance claims
require measurements; correctness improvements alone do not imply a speedup.
