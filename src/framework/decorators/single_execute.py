"""Renewable Redis single-execution lease for sync and async functions.

A lease cannot undo external effects after ownership is lost. Use downstream fencing
or idempotency where duplicate side effects are unacceptable.
"""

import asyncio
import functools
import inspect
import random
import threading
import time
import uuid

RENEW = "if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('EXPIRE',KEYS[1],ARGV[2]) end return 0"
RELEASE = (
    "if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0"
)


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
        if backend != "redis" or redis_client is None:
            raise ValueError("Redis is the supported distributed lock backend")
        if mode not in {"auto", "thread", "asyncio"}:
            raise ValueError("Use asyncio or thread mode")
        if (
            not 0 < renew_every_sec < run_ttl_sec
            or cooldown_sec < 0
            or (cooldown_on_error_sec is not None and cooldown_on_error_sec < 0)
        ):
            raise ValueError("Require 0 < renewal < TTL and nonnegative cooldown")
        self.r, self.key = redis_client, key_prefix + lock_id
        self.ttl, self.renewal = run_ttl_sec, renew_every_sec
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

    def __call__(self, fn):
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def wrapper(*args, **kwargs):
                await asyncio.sleep(random.uniform(0, self.jitter))
                token, started = uuid.uuid4().hex, time.monotonic()
                while not await self.r.set(self.key, token, nx=True, ex=self.ttl):
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
                            if not await self.r.eval(RENEW, 1, self.key, token, self.ttl):
                                lost = True
                                return
                    except Exception:
                        lost = True

                task = asyncio.create_task(renew())
                success = False
                try:
                    result = await fn(*args, **kwargs)
                    if lost:
                        raise RuntimeError("SingleExecute: lease lost")
                    success = True
                    return result
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    cooldown = (
                        self.cooldown
                        if success or self.error_cooldown is None
                        else self.error_cooldown
                    )
                    if cooldown:
                        await self.r.eval(RENEW, 1, self.key, token, cooldown)
                    elif self.release:
                        await self.r.eval(RELEASE, 1, self.key, token)
        else:

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                time.sleep(random.uniform(0, self.jitter))
                token, started = uuid.uuid4().hex, time.monotonic()
                while not self.r.set(self.key, token, nx=True, ex=self.ttl):
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
                            if not self.r.eval(RENEW, 1, self.key, token, self.ttl):
                                lost.set()
                                return
                    except Exception:
                        lost.set()

                thread = threading.Thread(target=renew, daemon=True)
                thread.start()
                success = False
                try:
                    result = fn(*args, **kwargs)
                    if lost.is_set():
                        raise RuntimeError("SingleExecute: lease lost")
                    success = True
                    return result
                finally:
                    stop.set()
                    thread.join(timeout=self.renewal + 1)
                    cooldown = (
                        self.cooldown
                        if success or self.error_cooldown is None
                        else self.error_cooldown
                    )
                    if cooldown:
                        self.r.eval(RENEW, 1, self.key, token, cooldown)
                    elif self.release:
                        self.r.eval(RELEASE, 1, self.key, token)

        return wrapper
