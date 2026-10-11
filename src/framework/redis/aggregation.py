import asyncio
import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass

from redis.asyncio import Redis

from ..commons.utils import deep_merge

# Related keys share a cluster hash tag. Refuse to mutate parts while a lease exists.
CLAIM = """
if redis.call('EXISTS', KEYS[3]) == 1 then return {'done'} end
if redis.call('EXISTS', KEYS[2]) == 1 then return {'busy'} end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
if redis.call('HLEN', KEYS[1]) < tonumber(ARGV[4]) then return {'stored'} end
redis.call('SET', KEYS[2], ARGV[5], 'EX', ARGV[6])
local parts = redis.call('HGETALL', KEYS[1])
table.insert(parts, 1, 'ready')
return parts
"""
RENEW = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('EXPIRE', KEYS[1], ARGV[2])
redis.call('EXPIRE', KEYS[2], ARGV[3])
return 1
"""
FINISH = """
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[3], '1', 'EX', ARGV[2])
redis.call('DEL', KEYS[1], KEYS[2])
return 1
"""
RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""


@dataclass
class Claim:
    status: str
    keys: list[str]
    token: str
    payload: dict | None = None
    ttl: int = 600
    lost: bool = False


class AggregationStore:
    def __init__(self, settings, client=None):
        self.settings = settings
        self.client = client or Redis.from_url(
            settings.redis_url.get_secret_value(),
            decode_responses=True,
            max_connections=settings.redis_max_connections,
            socket_timeout=settings.redis_timeout_sec,
            socket_connect_timeout=settings.redis_timeout_sec,
        )
        self._owned = client is None

    async def start(self):
        await self.client.ping()

    def keys(self, spec, key):
        digest = hashlib.sha256(
            json.dumps([self.settings.consumer_name, spec.name, key]).encode()
        ).hexdigest()
        return [f"qf:agg:{{{digest}}}:{suffix}" for suffix in ("parts", "lease", "done")]

    async def claim(self, spec, key, topic, payload):
        if topic not in spec.topics_in:
            raise ValueError("Unknown aggregation topic")
        token, keys = uuid.uuid4().hex, self.keys(spec, key)
        result = await self.client.eval(
            CLAIM,
            3,
            *keys,
            topic,
            json.dumps(payload),
            spec.aggregator_timeout_sec,
            len(spec.topics_in),
            token,
            self.settings.aggregate_lease_sec,
        )
        status = result[0]
        claim = Claim(status, keys, token, ttl=spec.aggregator_timeout_sec)
        if status == "ready":
            parts = dict(zip(result[1::2], result[2::2]))
            merged = {}
            for source in spec.topics_in:
                deep_merge(merged, json.loads(parts[source]))
            claim.payload = merged
        return claim

    @asynccontextmanager
    async def heartbeat(self, claim):
        async def renew():
            while True:
                await asyncio.sleep(self.settings.aggregate_lease_sec / 3)
                try:
                    ok = await self.client.eval(
                        RENEW,
                        2,
                        claim.keys[1],
                        claim.keys[0],
                        claim.token,
                        self.settings.aggregate_lease_sec,
                        claim.ttl,
                    )
                except Exception:
                    claim.lost = True
                    return
                if not ok:
                    claim.lost = True
                    return

        task = asyncio.create_task(renew(), name="qf-aggregate-lease")
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def finish(self, claim, ttl):
        if not await self.client.eval(FINISH, 3, *claim.keys, claim.token, ttl):
            raise RuntimeError("Aggregate lease lost before completion")

    async def release(self, claim):
        await self.client.eval(RELEASE, 1, claim.keys[1], claim.token)

    async def stop(self):
        if self._owned:
            await self.client.aclose()
