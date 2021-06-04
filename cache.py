from motor.motor_asyncio import AsyncIOMotorClient
import datetime
import json
import aioredis
import asyncio
import logging


DEFAULT_EXPIRY = datetime.timedelta(days=30)
DEFAULT_EVICTION_LOOP = datetime.timedelta(days=1)
DEFAULT_KEY_PREFIX = "fpc"
DEFAULT_SERIALIZER = json.dumps
DEFAULT_DESERIALIZER = json.loads

logger = logging.getLogger(__name__)


class PersistentCache:
    def __init__(self):
        self.client = AsyncIOMotorClient()

    @property
    def _db(self):
        return self.client["fpc_cache"]

    async def init(self):
        asyncio.create_task(self.eviction_loop())

    async def eviction_loop(self, interval: datetime.timedelta = DEFAULT_EVICTION_LOOP):
        while True:
            logger.info("Evicting expired mongo cache")
            res = await self._db.cache.delete_many(
                {"expiry": {"$lte": datetime.datetime.utcnow()}}
            )
            logger.info(f"Evicted {res.deleted_count} keys")
            await asyncio.sleep(interval.total_seconds())

    async def getJSONWithExpiry(self, key, deserializer=DEFAULT_DESERIALIZER):
        data = await self._db.cache.find_one({"key": key})
        logger.info("Persisted: getJSONWithExpiry %s", key)

        # if it doesn't exist
        if data is None:
            return None

        if datetime.datetime.utcnow() > data["expiry"]:
            await self._db.cache.delete_one({"key": key})
            return None
        else:
            return {"data": deserializer(data["data"]), "expiry": data["expiry"]}

    async def getJSON(self, key, deserializer=DEFAULT_DESERIALIZER):
        logger.info("Persisted: mongo: getJSON %s", key)

        data = await self.getJSONWithExpiry(key, deserializer=deserializer)
        if data is None:
            return None
        else:
            return data["data"]

    async def setJSON(
        self, key, data, expiry=DEFAULT_EXPIRY, serializer=DEFAULT_SERIALIZER
    ):
        logger.info("Persisted: mongo: setJSON %s %s %s", key, serializer(data), expiry)

        return await self._db.cache.find_one_and_update(
            {"key": key},
            {
                "$set": {
                    "data": serializer(data),
                    "expiry": datetime.datetime.utcnow() + expiry,
                }
            },
            upsert=True,
        )


class InMemoryCache:
    def __init__(self):
        self._client = None

    async def init(self):
        self._client = await aioredis.create_redis_pool("redis://localhost")

    async def getJSON(self, key, deserializer=DEFAULT_DESERIALIZER):
        key = f"{DEFAULT_KEY_PREFIX}:{key}"
        logger.info("InMemory: getJSON %s", key)
        data = await self._client.get(key, encoding="utf-8")
        if data is None:
            return None

        return deserializer(data)

    async def setJSON(
        self,
        key,
        data,
        expiry: datetime.timedelta = DEFAULT_EXPIRY,
        serializer=DEFAULT_SERIALIZER,
    ):
        key = f"{DEFAULT_KEY_PREFIX}:{key}"
        logger.info("InMemory: setJSON %s %s %s", key, serializer(data), expiry)
        return await self._client.set(
            key, serializer(data), expire=int(expiry.total_seconds())
        )


class Cache:
    def __init__(self):
        self._in_memory_cache = InMemoryCache()
        self._persistent_cache = PersistentCache()
        self._inited = False

    async def init(self):
        if not self._inited:
            await self._in_memory_cache.init()
            await self._persistent_cache.init()
            self._inited = True

    async def getJSON(self, key, deserializer=DEFAULT_DESERIALIZER):
        await self.init()
        data = await self._in_memory_cache.getJSON(key)
        if data is None:
            data = await self._persistent_cache.getJSONWithExpiry(
                key, deserializer=deserializer
            )

            if data is None:
                return None
            else:
                await self._in_memory_cache.setJSON(
                    key,
                    data["data"],
                    expiry=data["expiry"] - datetime.datetime.utcnow(),
                    serializer=DEFAULT_SERIALIZER,
                )
                return data["data"]

        else:
            return data

    async def setJSON(
        self, key, data, expiry=DEFAULT_EXPIRY, serializer=DEFAULT_SERIALIZER
    ):
        await self.init()
        await self._persistent_cache.setJSON(key, data, expiry, serializer)
        await self._in_memory_cache.setJSON(key, data, expiry, serializer)

    async def getCachedOrGetFromSourceAndCache(
        self, key, coro, expiry=DEFAULT_EXPIRY, serializer=DEFAULT_SERIALIZER
    ):
        data = await self.getJSON(key)
        if data is None:
            data = await coro
            await self.setJSON(key, data, expiry, serializer)
        return data
