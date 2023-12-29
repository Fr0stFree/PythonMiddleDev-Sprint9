import json

import redis

from .base import AbstractBroker


class RedisBroker(AbstractBroker):
    def __init__(self, host: str, port: int, channel_name: str) -> None:
        self._channel_name = channel_name
        self._connection = redis.Redis(host=host, port=port)
        print(f"Connected to redis. Ready to send messages to channel '{self._channel_name}'")

    def send(self, messages: list[dict]) -> None:
        self._connection.publish(self._channel_name, json.dumps(messages).encode('utf-8'))

    def is_connected(self) -> bool:
        return self._connection.ping()

    def disconnect(self) -> None:
        self._connection.close()
