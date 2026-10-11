"""Existing uppercase application configuration remains usable."""


class Config:
    WORKER_NAME = "qf-example"
    WORKER_MODULE = "examples.workers"
    KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
    ERROR_TOPIC = "qf.errors"
    REDIS_HOST = "localhost"
    REDIS_PORT = 26379
    REDIS_DB = 0
    QSINT_ENABLE_ETL = False
    QSINT_ENDPOINT_JSON = "maps/endpoint.json"
    QSINT_LOG_LEVEL = "INFO"
