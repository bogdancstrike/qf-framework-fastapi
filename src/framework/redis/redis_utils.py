"""Async Redis helper. Each instance owns its pool; every method must be awaited."""

from redis.asyncio import Redis


class RedisUtils:
    def __init__(
        self,
        host="localhost",
        port=6379,
        db=0,
        password=None,
        max_connections=50,
        socket_timeout=5,
        socket_connect_timeout=5,
        retry_on_timeout=False,
        *,
        url=None,
    ):
        args = dict(
            decode_responses=True,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
        )
        self.redis = (
            Redis.from_url(url, **args)
            if url
            else Redis(host=host, port=port, db=db, password=password, **args)
        )

    async def __aenter__(self):
        await self.redis.ping()
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        await self.redis.aclose()

    async def set_key(self, key, value, expire=None):
        return await self.redis.set(key, value, ex=expire)

    async def get_key(self, key):
        return await self.redis.get(key)

    async def delete_key(self, key):
        return await self.redis.delete(key)

    async def exists_key(self, key):
        return bool(await self.redis.exists(key))

    async def increment_key(self, key, amount=1):
        return await self.redis.incr(key, amount)

    async def set_hash(self, name, key, value):
        return await self.redis.hset(name, key, value)

    async def get_hash(self, name, key):
        return await self.redis.hget(name, key)

    async def delete_hash_field(self, name, key):
        return bool(await self.redis.hdel(name, key))

    async def get_all_hash(self, name):
        return await self.redis.hgetall(name)

    async def list_all_keys(self, pattern="*"):
        return [key async for key in self.redis.scan_iter(match=pattern)]

    async def get_list_length(self, key):
        return await self.redis.llen(key)
