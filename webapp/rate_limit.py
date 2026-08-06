from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import Protocol


class RateLimitExceeded(RuntimeError):
    pass


class RateLimiter(Protocol):
    async def check_minute(self, user_id: str) -> None:
        ...

    async def acquire_turn(self, user_id: str) -> None:
        ...

    async def release_turn(self, user_id: str) -> None:
        ...


class InMemoryRateLimiter:
    """Single-process fallback used when AKASHIC_WEB_REDIS_URL is not configured."""

    def __init__(self, *, max_per_minute: int = 20, max_concurrent: int = 2) -> None:
        self.max_per_minute = max(1, int(max_per_minute))
        self.max_concurrent = max(1, int(max_concurrent))
        self._recent: dict[str, deque[float]] = defaultdict(deque)
        self._active: dict[str, int] = defaultdict(int)

    async def check_minute(self, user_id: str) -> None:
        now = time.monotonic()
        bucket = self._recent[user_id]
        while bucket and now - bucket[0] > 60:
            bucket.popleft()
        if len(bucket) >= self.max_per_minute:
            raise RateLimitExceeded("rate limit exceeded")
        bucket.append(now)

    async def acquire_turn(self, user_id: str) -> None:
        if self._active[user_id] >= self.max_concurrent:
            raise RateLimitExceeded("too many concurrent turns")
        self._active[user_id] += 1

    async def release_turn(self, user_id: str) -> None:
        self._active[user_id] = max(0, self._active[user_id] - 1)


class RedisRateLimiter:
    """Redis-backed limiter for multi-process API deployments."""

    def __init__(
        self,
        redis_url: str,
        *,
        max_per_minute: int = 20,
        max_concurrent: int = 2,
    ) -> None:
        try:
            from redis.asyncio import Redis
        except ImportError as exc:  # pragma: no cover - optional deploy dependency
            raise RuntimeError("redis package is required for AKASHIC_WEB_REDIS_URL") from exc

        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.max_per_minute = max(1, int(max_per_minute))
        self.max_concurrent = max(1, int(max_concurrent))

    async def check_minute(self, user_id: str) -> None:
        key = f"web:rate:{user_id}:{int(time.time() // 60)}"
        count = await self.redis.incr(key)
        if count == 1:
            await self.redis.expire(key, 70)
        if int(count) > self.max_per_minute:
            raise RateLimitExceeded("rate limit exceeded")

    async def acquire_turn(self, user_id: str) -> None:
        key = f"web:turns:active:{user_id}"
        count = await self.redis.incr(key)
        if count == 1:
            await self.redis.expire(key, 3600)
        if int(count) > self.max_concurrent:
            await self.redis.decr(key)
            raise RateLimitExceeded("too many concurrent turns")

    async def release_turn(self, user_id: str) -> None:
        key = f"web:turns:active:{user_id}"
        count = await self.redis.decr(key)
        if int(count) <= 0:
            await self.redis.delete(key)
