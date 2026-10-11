"""Sync/async call policies. Async wrappers never block the event loop."""

import asyncio
import functools
import inspect
import threading
import time

import tenacity


class RetryExhaustedError(tenacity.RetryError):
    pass


class CircuitOpenError(Exception):
    pass


class RateLimitExceededError(Exception):
    pass


def _registered(fn, wrapper):
    from dataclasses import replace
    from . import kafka_workers as kw

    name = getattr(fn, "_qsint_worker_name", None)
    if name in kw._WORKERS_BY_NAME:
        kw._WORKERS_BY_NAME[name] = replace(kw._WORKERS_BY_NAME[name], fn=wrapper)
    return wrapper


def retry(
    *,
    max_attempts=3,
    wait_fixed=1.0,
    wait_exponential=False,
    wait_multiplier=1.0,
    wait_max=60.0,
    exceptions=(Exception,),
    reraise=False,
):
    if max_attempts < 1 or min(wait_fixed, wait_multiplier, wait_max) < 0:
        raise ValueError("Invalid retry configuration")
    wait = (
        tenacity.wait_random_exponential(multiplier=wait_multiplier, max=wait_max)
        if wait_exponential
        else tenacity.wait_fixed(wait_fixed)
    )
    policy = tenacity.retry(
        stop=tenacity.stop_after_attempt(max_attempts),
        wait=wait,
        retry=tenacity.retry_if_exception_type(exceptions),
        reraise=reraise,
        retry_error_cls=RetryExhaustedError,
    )

    def decorate(fn):
        return _registered(fn, policy(fn))

    return decorate


def call_circuit_breaker(*, fail_max=5, reset_timeout=30, name=None, exclude=()):
    if fail_max < 1 or reset_timeout <= 0:
        raise ValueError("Invalid circuit configuration")

    def decorate(fn):
        lock = threading.Lock()
        state = {"failures": 0, "until": 0.0, "probe": False, "generation": 0}

        def enter():
            with lock:
                if state["until"] > time.monotonic() or state["probe"]:
                    raise CircuitOpenError(name or fn.__qualname__)
                if state["until"]:
                    state["probe"] = True
                    state["generation"] += 1
                return state["generation"]

        def leave(generation, error=None):
            with lock:
                if generation != state["generation"]:
                    return  # An older in-flight result cannot reset a newer circuit.
                if error is None or isinstance(error, exclude):
                    state.update(failures=0, until=0.0, probe=False)
                elif not isinstance(error, Exception):
                    state["probe"] = False
                else:
                    state["failures"] += 1
                    if state["probe"] or state["failures"] >= fail_max:
                        state["until"] = time.monotonic() + reset_timeout
                        state["generation"] += 1
                    state["probe"] = False

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def wrapper(*args, **kwargs):
                generation = enter()
                try:
                    result = await fn(*args, **kwargs)
                except BaseException as exc:
                    leave(generation, exc)
                    raise
                leave(generation)
                return result
        else:

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                generation = enter()
                try:
                    result = fn(*args, **kwargs)
                except BaseException as exc:
                    leave(generation, exc)
                    raise
                leave(generation)
                return result

        wrapper.__circuit_breaker__ = state
        return _registered(fn, wrapper)

    return decorate


def call_rate_limit(
    *,
    per_second=None,
    per_minute=None,
    per_hour=None,
    storage_url="memory://",
    on_exceeded="raise",
    key=None,
):
    from limits import parse
    from limits.storage import storage_from_string
    from limits.strategies import MovingWindowRateLimiter

    choices = [
        (n, unit, seconds)
        for n, unit, seconds in [
            (per_second, "second", 1),
            (per_minute, "minute", 60),
            (per_hour, "hour", 3600),
        ]
        if n is not None
    ]
    if len(choices) != 1 or choices[0][0] < 1 or int(choices[0][0]) != choices[0][0]:
        raise ValueError("Specify exactly one positive integer rate")
    if on_exceeded not in {"raise", "sleep"}:
        raise ValueError("Invalid on_exceeded")
    number, unit, seconds = choices[0]
    item = parse(f"{int(number)} per {unit}")
    limiter = MovingWindowRateLimiter(storage_from_string(storage_url))

    def decorate(fn):
        def bucket(args, kwargs):
            return (
                key(*args, **kwargs)
                if callable(key)
                else key or f"{fn.__module__}.{fn.__qualname__}"
            )

        def hit(name):
            return limiter.hit(item, name)

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def wrapper(*args, **kwargs):
                name = bucket(args, kwargs)
                while not await asyncio.to_thread(hit, name):
                    if on_exceeded == "raise":
                        raise RateLimitExceededError(str(name))
                    await asyncio.sleep(min(seconds, 0.1))
                return await fn(*args, **kwargs)
        else:

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                name = bucket(args, kwargs)
                while not hit(name):
                    if on_exceeded == "raise":
                        raise RateLimitExceededError(str(name))
                    time.sleep(min(seconds, 0.1))
                return fn(*args, **kwargs)

        return _registered(fn, wrapper)

    return decorate
