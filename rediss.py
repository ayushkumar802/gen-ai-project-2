#Decaration of REDIS
import redis.asyncio as aioredis
import json
import os

class CacheManager:
    def __init__(self):
        self.client = aioredis.Redis.from_url(
            os.getenv("REDIS_URL"),
            decode_responses=True
        )

    async def get(self, key: str):
        data = await self.client.get(key)
        return json.loads(data) if data else None

    async def set(self, key: str, value, ttl: int = 3600):
        await self.client.setex(key, ttl, json.dumps(value))

    async def delete(self, key: str):
        await self.client.delete(key)