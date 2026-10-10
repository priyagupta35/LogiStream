import logging
from typing import Optional, List, Tuple
import redis.asyncio as aioredis
from api.config import settings

logger = logging.getLogger("logistream.redis")

class RedisClient:
    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None

    async def connect(self):
        try:
            self._redis = aioredis.from_url(
                settings.REDIS_URL,
                decode_responses=True
            )
            await self._redis.ping()
            logger.info("Connected to Redis successfully")
        except Exception as e:
            logger.warning(f"Could not connect to Redis at {settings.REDIS_URL}: {e}")
            self._redis = None

    async def disconnect(self):
        if self._redis:
            await self._redis.close()
            logger.info("Redis connection closed")

    @property
    def client(self) -> Optional[aioredis.Redis]:
        return self._redis

    async def update_driver_location(self, driver_id: str, lat: float, lng: float):
        """Adds or updates driver coordinate in Redis GEO set."""
        if not self._redis:
            return
        # Redis GEO expects (key, longitude, latitude, member)
        await self._redis.geoadd(
            settings.REDIS_DRIVER_GEO_KEY,
            (lng, lat, driver_id)
        )
        # Update driver status TTL (e.g. heartbeat expires in 120 seconds if inactive)
        await self._redis.hset(
            f"driver:{driver_id}:info",
            mapping={"lat": str(lat), "lng": str(lng), "status": "AVAILABLE"}
        )
        await self._redis.expire(f"driver:{driver_id}:info", 120)

    async def find_nearest_driver(self, pickup_lat: float, pickup_lng: float, radius_km: float = 25.0) -> Optional[str]:
        """
        Uses Redis GEOSEARCH to discover the closest driver within radius_km.
        Returns driver_id or None.
        """
        if not self._redis:
            return None
        try:
            # Redis GEOSEARCH with distance ascending
            results = await self._redis.geosearch(
                name=settings.REDIS_DRIVER_GEO_KEY,
                longitude=pickup_lng,
                latitude=pickup_lat,
                radius=radius_km,
                unit="km",
                sort="ASC",
                count=5
            )
            # Find first available driver
            for driver_id in results:
                status = await self._redis.hget(f"driver:{driver_id}:info", "status")
                if status == "AVAILABLE" or status is None:
                    return driver_id
            return results[0] if results else None
        except Exception as e:
            logger.error(f"Error executing Redis GEOSEARCH: {e}")
            return None

    async def set_driver_busy(self, driver_id: str):
        if self._redis:
            await self._redis.hset(f"driver:{driver_id}:info", "status", "BUSY")
            # Remove from active GEO set temporarily while on a delivery
            await self._redis.zrem(settings.REDIS_DRIVER_GEO_KEY, driver_id)

    async def set_driver_available(self, driver_id: str, lat: float, lng: float):
        if self._redis:
            await self._redis.hset(f"driver:{driver_id}:info", "status", "AVAILABLE")
            await self._redis.geoadd(settings.REDIS_DRIVER_GEO_KEY, (lng, lat, driver_id))

redis_client = RedisClient()
