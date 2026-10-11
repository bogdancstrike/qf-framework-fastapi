"""Async stream client contract retained at the original import path."""

from abc import ABC, abstractmethod


class StreamClientInterface(ABC):
    @abstractmethod
    async def create_topic(
        self, topic_name, num_partitions=1, replication_factor=1, retention_time="86400000"
    ): ...

    @abstractmethod
    async def put_message(self, topic_name, message, key=None): ...

    @abstractmethod
    async def consume_message_by_key(self, topic_name, key, **kwargs): ...

    @abstractmethod
    async def consume_message(self, topic_name, **kwargs): ...

    @abstractmethod
    async def delete_topic(self, topic_name): ...

    @abstractmethod
    async def topic_exists(self, topic_name): ...
