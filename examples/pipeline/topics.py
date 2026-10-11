"""Use a separate prefix for each concurrent demo or integration test."""

import os
import re

PREFIX = os.getenv("QF_DEMO_PREFIX", "qf.demo")
if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,120}", PREFIX):
    raise ValueError("QF_DEMO_PREFIX must be 1-121 Kafka-safe characters")

INPUT = f"{PREFIX}.jobs.in"
STATS_INPUT = f"{PREFIX}.stats.in"
PREVIEW_INPUT = f"{PREFIX}.preview.in"
STATS_OUTPUT = f"{PREFIX}.stats.out"
PREVIEW_OUTPUT = f"{PREFIX}.preview.out"
OUTPUT = f"{PREFIX}.jobs.done"
DLQ = f"{PREFIX}.dlq"
ALL = [INPUT, STATS_INPUT, PREVIEW_INPUT, STATS_OUTPUT, PREVIEW_OUTPUT, OUTPUT, DLQ]
