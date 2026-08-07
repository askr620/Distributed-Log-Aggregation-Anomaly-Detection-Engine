"""Redis sliding-window counters for log anomaly detection."""

import time

from redis.asyncio import Redis


class RedisSlidingWindow:
    """Count log events by service, level, and time bucket in Redis."""

    def __init__(self, redis: Redis, window_size_seconds: int = 60) -> None:
        self.redis = redis
        self.window_size_seconds = window_size_seconds

    def current_bucket(self) -> int:
        """Return the current bucket number."""
        return int(time.time()) // self.window_size_seconds

    def key_for(self, service: str, level: str, bucket: int) -> str:
        """Build the Redis key for a service/level/time bucket."""
        safe_service = service.replace(" ", "_")
        safe_level = level.replace(" ", "_")
        return f"log:{safe_service}:{safe_level}:{bucket}"

    async def increment_current(self, service: str, level: str) -> tuple[int, int]:
        """Increment the current bucket and return bucket plus new count."""
        bucket = self.current_bucket()
        key = self.key_for(service, level, bucket)
        count = await self.redis.incr(key)
        await self.redis.expire(key, self.window_size_seconds * 20)
        return bucket, int(count)

    async def get_previous_counts(self, service: str, level: str, current_bucket: int, count: int = 10) -> list[int]:
        """Fetch counts from previous buckets, oldest first."""
        buckets = range(current_bucket - count, current_bucket)
        keys = [self.key_for(service, level, bucket) for bucket in buckets]
        values = await self.redis.mget(keys)
        return [int(value) if value is not None else 0 for value in values]
