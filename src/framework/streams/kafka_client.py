"""Explicit async Kafka convenience client; no per-message connections."""

import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from ..tracing import inject_trace_headers
from .stream_interface import StreamClientInterface


class KafkaClient(StreamClientInterface):
    def __init__(self, bootstrap_servers="localhost:9092", **security):
        self.bootstrap_servers, self.security = bootstrap_servers, security
        self.producer = None

    async def __aenter__(self):
        if self.producer is not None:
            raise RuntimeError("KafkaClient is already entered")
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers, enable_idempotence=True, **self.security
        )
        try:
            await self.producer.start()
        except BaseException:
            try:
                await self.producer.stop()
            finally:
                self.producer = None
            raise
        return self

    async def __aexit__(self, *args):
        if self.producer:
            try:
                await self.producer.stop()
            finally:
                self.producer = None

    async def put_message(self, topic_name, message, key=None):
        if self.producer is None:
            raise RuntimeError("Use KafkaClient with async with")
        value = (
            json.dumps(message, allow_nan=False).encode()
            if not isinstance(message, bytes)
            else message
        )
        key = key.encode() if isinstance(key, str) else key
        return await self.producer.send_and_wait(
            topic_name, value, key=key, headers=inject_trace_headers()
        )

    async def consume_message(
        self, topic_name, group_id=None, auto_offset_reset="earliest", timeout_ms=10000
    ):
        consumer = AIOKafkaConsumer(
            topic_name,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset=auto_offset_reset,
            **self.security,
        )
        try:
            await consumer.start()
            return await consumer.getmany(timeout_ms=timeout_ms)
        finally:
            await consumer.stop()

    async def consume_message_by_key(self, topic_name, key, **kwargs):
        key = key.encode() if isinstance(key, str) else key
        records = await self.consume_message(topic_name, **kwargs)
        return [m for batch in records.values() for m in batch if m.key == key]

    async def admin(self, operation):
        client = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers, **self.security)
        try:
            await client.start()
            return await operation(client)
        finally:
            await client.close()

    async def create_topic(
        self, topic_name, num_partitions=1, replication_factor=1, retention_time="86400000"
    ):
        return await self.admin(
            lambda client: client.create_topics(
                [
                    NewTopic(
                        topic_name,
                        num_partitions,
                        replication_factor,
                        topic_configs={"retention.ms": str(retention_time)},
                    )
                ]
            )
        )

    async def delete_topic(self, topic_name):
        return await self.admin(lambda client: client.delete_topics([topic_name]))

    async def topic_exists(self, topic_name):
        return topic_name in await self.admin(lambda client: client.list_topics())
