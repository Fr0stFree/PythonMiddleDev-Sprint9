import logging

from redis.asyncio import Redis

from .base import AbstractBroker


logger = logging.getLogger(__name__)


class RedisBroker(AbstractBroker):
    def __init__(self, host: str, port: int, channel_name: str) -> None:
        self._channel_name = channel_name
        self._connection = Redis(host=host, port=port)

    async def connect(self) -> None:
        is_connected = await self._connection.ping()
        if not is_connected:
            raise ConnectionError("Could not connect to redis")
        logger.info(f"Connected to redis. Ready to send messages to channel '{self._channel_name}'")

    async def send(self, message: bytes) -> None:
        await self._connection.publish(self._channel_name, message)

    async def disconnect(self) -> None:
        await self._connection.close()
