"""Run: python -m examples.single_execute; requires the development Redis."""

import asyncio
import os
import uuid

from redis.asyncio import Redis

from framework.decorators.single_execute import SingleExecute


async def main():
    lock_id = "demo-" + uuid.uuid4().hex
    async with Redis.from_url(
        os.getenv("QSINT_REDIS_URL", "redis://localhost:26379/0"), socket_timeout=2
    ) as client:
        executions = 0

        @SingleExecute(
            lock_id=lock_id,
            redis_client=client,
            run_ttl_sec=3,
            renew_every_sec=0.5,
            cooldown_sec=30,
            cooldown_on_error_sec=0,
        )
        async def refresh():
            nonlocal executions
            executions += 1
            await asyncio.sleep(0.1)
            return "executed"

        try:
            results = await asyncio.gather(*(refresh() for _ in range(5)))
            assert executions == 1 and results.count("executed") == 1
            print({"executions": executions, "skipped": results.count(None)})
        finally:
            # Unique demonstration key; production leaves cooldown until expiry.
            await client.delete("singleexec:" + lock_id)


if __name__ == "__main__":
    asyncio.run(main())
