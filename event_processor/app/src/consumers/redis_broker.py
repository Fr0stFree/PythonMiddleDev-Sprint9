from typing import Generator, Union

import redis

from .base import AbstractBroker


class RedisBroker(AbstractBroker):
    def __init__(self, host: str, port: int, channel_name: str) -> None:
        self._channel_name = channel_name
        self._connection = redis.Redis(host=host, port=port)
        print(f"Connected to redis. Ready to receive messages from channel '{self._channel_name}'")

    def receive(self) -> Generator[Union[str, bytes], None, None]:
        pubsub = self._connection.pubsub()
        pubsub.subscribe(self._channel_name)

        for message in pubsub.listen():
            if message['type'] == 'message':
                yield message['data']

    def ack(self) -> None:
        pass
