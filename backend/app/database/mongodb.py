"""Cliente MongoDB — patrón Singleton."""

from __future__ import annotations

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.config import get_settings


class MongoDB:
    _instance: MongoDB | None = None
    _client: AsyncIOMotorClient | None = None

    def __new__(cls) -> MongoDB:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def connect(self) -> None:
        if self._client is not None:
            return
        settings = get_settings()
        if not settings.mongodb_uri:
            raise RuntimeError(
                "MONGODB_ATLAS_URI no configurada. Defínela en .env en la raíz del proyecto."
            )
        self._client = AsyncIOMotorClient(settings.mongodb_uri)

    async def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    @property
    def client(self) -> AsyncIOMotorClient:
        if self._client is None:
            self.connect()
        assert self._client is not None
        return self._client

    def get_database(self) -> AsyncIOMotorDatabase:
        settings = get_settings()
        return self.client[settings.mongodb_database]


mongodb = MongoDB()


async def get_db() -> AsyncIOMotorDatabase:
    return mongodb.get_database()
