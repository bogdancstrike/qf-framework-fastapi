"""Token-fenced Redis execution leases for matching sync/async clients.

The supplied Redis client remains application-owned. A lease cannot retract user
side effects after ownership loss; business operations still require idempotency.
"""

import asyncio
import functools
import inspect
import math
import random
import threading
import time
import uuid

from redis.asyncio import Redis as AsyncRedis

from ..commons.logger import logger
from .common import _registered

RENEW = "if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE',KEYS[1],ARGV[2]) end return 0"
FINISH = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if tonumber(ARGV[2]) > 0 then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
if ARGV[3] == '1' then return redis.call('DEL', KEYS[1]) end
return 1
"""


class SingleExecute:
    def __init__(
        self,
        *,
        lock_id,
        redis_client=None,
        backend="redis",
        key_prefix="singleexec:",
        mode="auto",
        run_ttl_sec=240,
        renew_every_sec=30,
        cooldown_sec=300,
        cooldown_on_error_sec=None,
        jitter_sec=0,
        block=False,
        block_timeout_sec=None,
        raise_if_not_acquired=False,
        on_skip_return=None,
        release_on_cooldown_zero=True,
        **kwargs,
    ):
        if kwargs:
            raise TypeError(f"Unsupported SingleExecute options: {', '.join(sorted(kwargs))}")
        if backend != "redis" or redis_client is None:
            raise ValueError("Redis is the supported distributed lock backend")
        if mode not in {"auto", "thread", "asyncio"}:
            raise ValueError("Use auto, asyncio or thread mode")
        values = [run_ttl_sec, renew_every_sec, cooldown_sec, jitter_sec]
        values += [v for v in (cooldown_on_error_sec, block_timeout_sec) if v is not None]
        if any(not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0 for v in values):
            raise ValueError("Lease durations must be finite and nonnegative")
        if not 0 < renew_every_sec < run_ttl_sec or run_ttl_sec < 0.001:
            raise ValueError("Require 0 < renewal < TTL, with TTL at least one millisecond")
        if not isinstance(lock_id, str) or not lock_id or not isinstance(key_prefix, str):
            raise ValueError("lock_id must be a nonempty string and key_prefix a string")
        self.r, self.key, self.mode = redis_client, key_prefix + lock_id, mode
        self.ttl_ms, self.renewal = math.ceil(run_ttl_sec * 1000), renew_every_sec
        self.cooldown, self.error_cooldown = cooldown_sec, cooldown_on_error_sec
        self.jitter, self.block, self.timeout = jitter_sec, block, block_timeout_sec
        self.raise_skip, self.skip, self.release = (
            raise_if_not_acquired,
            on_skip_return,
            release_on_cooldown_zero,
        )

    def skipped(self):
        if self.raise_skip:
            raise RuntimeError("SingleExecute: lock not acquired")
        return self.skip

    def cooldown_ms(self, success):
        seconds = self.cooldown if success or self.error_cooldown is None else self.error_cooldown
        return math.ceil(seconds * 1000)

    def __call__(self, fn):
        asynchronous = inspect.iscoroutinefunction(fn)
        async_client = isinstance(self.r, AsyncRedis) or inspect.iscoroutinefunction(
            getattr(self.r, "execute_command", getattr(self.r, "set", None))
        )
        if asynchronous != async_client:
            raise ValueError(
                "SingleExecute requires an async Redis client for async functions and a sync client for sync functions"
            )
        if self.mode != "auto" and (self.mode == "asyncio") != asynchronous:
            raise ValueError(
                "SingleExecute mode must match the decorated function and Redis client"
            )

        if asynchronous:

            @functools.wraps(fn)
            async def wrapper(*args, **kwargs):
                if self.jitter:
                    await asyncio.sleep(random.uniform(0, self.jitter))
                token, started = uuid.uuid4().hex, time.monotonic()
                while not await self.r.set(self.key, token, nx=True, px=self.ttl_ms):
                    if (
                        not self.block
                        or self.timeout is not None
                        and time.monotonic() - started >= self.timeout
                    ):
                        return self.skipped()
                    await asyncio.sleep(0.1)
                lost = False

                async def renew():
                    nonlocal lost
                    try:
                        while True:
                            await asyncio.sleep(self.renewal)
                            if not await self.r.eval(RENEW, 1, self.key, token, self.ttl_ms):
                                lost = True
                                return
                    except Exception:
                        lost = True

                task = asyncio.create_task(renew(), name="qf-single-execute-renewal")

                async def stop_renewal():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

                async def finish(success):
                    return await self.r.eval(
                        FINISH, 1, self.key, token, self.cooldown_ms(success), int(self.release)
                    )

                try:
                    result = await fn(*args, **kwargs)
                except BaseException:
                    await stop_renewal()
                    try:
                        await finish(False)
                    except Exception:
                        logger.warning("SingleExecute cleanup failed; preserving function error")
                    raise
                else:
                    await stop_renewal()
                    # Atomic token verification catches loss even before the first renewal.
                    if not await finish(not lost) or lost:
                        raise RuntimeError("SingleExecute: lease lost")
                    return result
                finally:
                    await stop_renewal()
        else:

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                if self.jitter:
                    time.sleep(random.uniform(0, self.jitter))
                token, started = uuid.uuid4().hex, time.monotonic()
                while not self.r.set(self.key, token, nx=True, px=self.ttl_ms):
                    if (
                        not self.block
                        or self.timeout is not None
                        and time.monotonic() - started >= self.timeout
                    ):
                        return self.skipped()
                    time.sleep(0.1)
                stop, lost = threading.Event(), threading.Event()

                def renew():
                    try:
                        while not stop.wait(self.renewal):
                            if not self.r.eval(RENEW, 1, self.key, token, self.ttl_ms):
                                lost.set()
                                return
                    except Exception:
                        lost.set()

                thread = threading.Thread(
                    target=renew, daemon=True, name="qf-single-execute-renewal"
                )
                thread.start()

                def stop_renewal():
                    stop.set()
                    thread.join(timeout=max(0.1, min(self.renewal, 1)))
                    if thread.is_alive():
                        lost.set()

                def finish(success):
                    return self.r.eval(
                        FINISH, 1, self.key, token, self.cooldown_ms(success), int(self.release)
                    )

                try:
                    result = fn(*args, **kwargs)
                except BaseException:
                    stop_renewal()
                    try:
                        if not thread.is_alive():
                            finish(False)
                    except Exception:
                        logger.warning("SingleExecute cleanup failed; preserving function error")
                    raise
                else:
                    stop_renewal()
                    if thread.is_alive() or not finish(not lost.is_set()) or lost.is_set():
                        raise RuntimeError("SingleExecute: lease lost")
                    return result
                finally:
                    stop_renewal()

        return _registered(fn, wrapper)
