from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class Metrics:
    """Each application owns its registry. Never label by offset, message or request ID."""

    def __init__(self):
        self.registry = CollectorRegistry()

        def counter(name, desc, labels):
            return Counter(name, desc, labels, registry=self.registry)

        def gauge(name, desc, labels):
            return Gauge(name, desc, labels, registry=self.registry)

        self.requests = counter(
            "qf_http_requests_total", "HTTP responses", ["method", "route", "status"]
        )
        self.http_duration = Histogram(
            "qf_http_duration_seconds", "HTTP duration", ["method", "route"], registry=self.registry
        )
        self.processed = counter(
            "qf_messages_total", "Completed source messages", ["worker", "outcome"]
        )
        self.retries = counter("qf_retries_total", "Retries", ["worker", "kind"])
        self.output_acks = counter(
            "qf_output_acks_total", "Acknowledged Kafka publications", ["worker", "kind"]
        )
        self.output_ack_duration = Histogram(
            "qf_output_ack_duration_seconds",
            "Time from enqueue to observed Kafka ACK",
            ["worker", "kind"],
            registry=self.registry,
        )
        self.idempotency = counter(
            "qf_idempotency_total", "Source ledger decisions and waits", ["worker", "outcome"]
        )
        self.aggregate_parts = counter(
            "qf_aggregate_parts_total", "Redis join claim outcomes", ["worker", "status"]
        )
        self.failures = counter("qf_failures_total", "Failures", ["component"])
        self.job_duration = Histogram(
            "qf_job_duration_seconds",
            "Worker job duration including retries",
            ["worker"],
            registry=self.registry,
        )
        self.pending = gauge(
            "qf_pending_records", "Queued records", ["worker", "topic", "partition"]
        )
        self.inflight = gauge("qf_inflight_jobs", "Running jobs", ["worker"])
        self.uncommitted = gauge(
            "qf_uncommitted_records", "Fetched records awaiting commit", ["topic", "partition"]
        )
        self.lag = gauge(
            "qf_consumer_lag",
            "Broker highwater minus acknowledged commit offset",
            ["topic", "partition"],
        )
        self.circuit = gauge("qf_circuit_open", "Circuit open or half-open", ["worker"])
        self.ready = gauge("qf_runtime_ready", "ETL ready", [])
        self.loop_lag = gauge("qf_event_loop_lag_seconds", "Scheduler wakeup delay", [])
