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
local owner = redis.call('GET', KEYS[2])
if owner then
  if owner ~= ARGV[5] then return {'busy'} end
  local parts = redis.call('HGETALL', KEYS[1])
  table.insert(parts, 1, 'ready')
  return parts
end
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
if redis.call('GET', KEYS[3]) == ARGV[1] then return 1 end
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[3], ARGV[1], 'EX', ARGV[2])
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
    finished: bool = False


@dataclass
class ClaimRequest:
    spec: object
    claim: Claim
    args: list


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
        self._claim_script = self.client.register_script(CLAIM)
        self._renew_script = self.client.register_script(RENEW)
        self._finish_script = self.client.register_script(FINISH)
        self._release_script = self.client.register_script(RELEASE)

    async def start(self):
        await self.client.ping()

    def keys(self, spec, key):
        digest = hashlib.sha256(
            json.dumps([self.settings.consumer_name, spec.name, key]).encode()
        ).hexdigest()
        return [f"qf:agg:{{{digest}}}:{suffix}" for suffix in ("parts", "lease", "done")]

    def prepare_claim(self, spec, key, topic, payload, *, token=None):
        if topic not in spec.topics_in:
            raise ValueError("Unknown aggregation topic")
        token, keys = token or uuid.uuid4().hex, self.keys(spec, key)
        return ClaimRequest(
            spec,
            Claim("unknown", keys, token, ttl=spec.aggregator_timeout_sec),
            [
                topic,
                json.dumps(payload, allow_nan=False),
                spec.aggregator_timeout_sec,
                len(spec.topics_in),
                token,
                self.settings.aggregate_lease_sec,
            ],
        )

    def decode_claim(self, request, result):
        claim, spec = request.claim, request.spec
        claim.status = result[0]
        if claim.status == "ready":
            parts = dict(zip(result[1::2], result[2::2]))
            merged = {}
            for source in spec.topics_in:
                deep_merge(merged, json.loads(parts[source]))
            claim.payload = merged
        return claim

    async def claim(self, spec, key, topic, payload, *, token=None):
        request = self.prepare_claim(spec, key, topic, payload, token=token)
        result = await self._claim_script(keys=request.claim.keys, args=request.args)
        return self.decode_claim(request, result)

    async def claim_many(self, requests):
        async with self.client.pipeline(transaction=False) as pipeline:
            for request in requests:
                await self._claim_script(
                    keys=request.claim.keys, args=request.args, client=pipeline
                )
            results = await pipeline.execute()
        return [self.decode_claim(request, result) for request, result in zip(requests, results)]

    @asynccontextmanager
    async def heartbeat(self, claim):
        async with self.heartbeat_many([claim]):
            yield

    @asynccontextmanager
    async def heartbeat_many(self, claims):
        """One renewal task per job; `claims` may grow while acquiring a bulk job."""

        async def renew():
            while True:
                await asyncio.sleep(self.settings.aggregate_lease_sec / 3)
                active = [
                    c for c in claims if c.status == "ready" and not c.finished and not c.lost
                ]
                if not active:
                    continue
                try:
                    async with self.client.pipeline(transaction=False) as pipeline:
                        for claim in active:
                            await self._renew_script(
                                keys=[claim.keys[1], claim.keys[0]],
                                args=[claim.token, self.settings.aggregate_lease_sec, claim.ttl],
                                client=pipeline,
                            )
                        results = await pipeline.execute()
                except Exception:
                    for claim in active:
                        claim.lost = True
                    return
                for claim, ok in zip(active, results):
                    if not ok and not claim.finished:
                        claim.lost = True

        task = asyncio.create_task(renew(), name="qf-aggregate-lease")
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def finish(self, claim, ttl):
        await self.finish_many([claim], ttl)

    async def finish_many(self, claims, ttl):
        if not claims:
            return
        async with self.client.pipeline(transaction=False) as pipeline:
            for claim in claims:
                await self._finish_script(keys=claim.keys, args=[claim.token, ttl], client=pipeline)
            results = await pipeline.execute()
        lost = False
        for claim, ok in zip(claims, results):
            if ok:
                claim.finished = True
            else:
                claim.lost = True
                lost = True
        if lost:
            raise RuntimeError("Aggregate lease lost before completion")

    async def release(self, claim):
        await self.release_many([claim])

    async def release_many(self, claims):
        active = [c for c in claims if not c.finished and c.status in {"ready", "unknown"}]
        if not active:
            return
        async with self.client.pipeline(transaction=False) as pipeline:
            for claim in active:
                await self._release_script(
                    keys=[claim.keys[1]], args=[claim.token], client=pipeline
                )
            await pipeline.execute()

    async def stop(self):
        if self._owned:
            await self.client.aclose()
