import os
from redis.asyncio import Redis  # Асинхронный клиент
from dotenv import load_dotenv

load_dotenv()
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))



class RedisWrapper:
    def __init__(self):
        self.redis = Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=0,
            decode_responses=True # автоматически преобразование с байт в строку
        )

    async def __aenter__(self):
        return self.redis

    async def __aexit__(self, exc_type, exc, tb):
        await self.redis.aclose()
