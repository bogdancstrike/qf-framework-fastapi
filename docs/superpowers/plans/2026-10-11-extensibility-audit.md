# Runtime correctness and extensibility audit

The objective is to reproduce and fix remaining concurrency, lifecycle and
configuration bugs without changing the public worker/configuration conventions.
Historical benchmark results remain attributed to their measured revisions.

- [ ] Reproduce stale circuit completions and cancelled half-open probes.
- [ ] Verify aggregation retention during active leases and recovery.
- [ ] Audit explicit worker registration, settings and endpoint validation.
- [ ] Audit application startup/shutdown resource ownership and cleanup.
- [ ] Add focused regression tests and document state-machine invariants.
- [ ] Document extension points and failure contracts in docs and README.
- [ ] Run unit, real-service, recovery and telemetry checks on Python 3.12/3.15.
- [ ] Commit and push verified changes; record evidence and remaining limits.

Use small, composable changes. Prefer existing adapters, policies and constructor
injection to new abstractions without a concrete use case. Performance claims
require measurements; correctness improvements alone do not imply a speedup.
