from typing import Generator

from pika import BlockingConnection, URLParameters
from pika.exchange_type import ExchangeType

from .base import AbstractBroker, IncomingMessage


class RabbitBroker(AbstractBroker):
    def __init__(self, user: str, password: str, host: str, port: int, exchange_name: str, queue_name: str) -> None:
        self._queue_name = queue_name
        self._exchange_name = exchange_name
        self._connection = BlockingConnection(URLParameters(f"amqp://{user}:{password}@{host}:{port}"))
        self._channel = self._connection.channel()
        self._messages_to_ack = []

    def receive(self) -> Generator[IncomingMessage, None, None]:
        self._channel.exchange_declare(exchange=self._exchange_name, exchange_type=ExchangeType.direct, durable=True)
        self._channel.queue_declare(queue=self._queue_name, durable=True)
        self._channel.queue_bind(exchange=self._exchange_name, queue=self._queue_name)
        print(f"Connected to rabbit."
              f"Ready to receive messages from queue '{self._queue_name}' through exchange '{self._exchange_name}'")

        for method_frame, properties, body in self._channel.consume(self._queue_name):
            self._messages_to_ack.append(method_frame.delivery_tag)
            yield body

    def ack(self) -> None:
        [self._channel.basic_ack(delivery_tag=tag) for tag in self._messages_to_ack]
        self._messages_to_ack.clear()
