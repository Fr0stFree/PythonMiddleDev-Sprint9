from typing import Generator, Union

from pika import BlockingConnection, URLParameters
from pika.exchange_type import ExchangeType

from .base import AbstractBroker


class RabbitBroker(AbstractBroker):
    def __init__(self, user: str, password: str, host: str, port: int, exchange_name: str, queue_name: str) -> None:
        self._queue_name = queue_name
        self._exchange_name = exchange_name
        self._connection = BlockingConnection(URLParameters(f"amqp://{user}:{password}@{host}:{port}"))
        self._channel = self._connection.channel()

    def receive(self) -> Generator[Union[str, bytes], None, None]:
        self._channel.exchange_declare(exchange=self._exchange_name, exchange_type=ExchangeType.direct, durable=True)
        self._channel.queue_declare(queue=self._queue_name, durable=True)
        self._channel.queue_bind(exchange=self._exchange_name, queue=self._queue_name)
        print(f"Connected to rabbit."
              f"Ready to receive messages from queue '{self._queue_name}' through exchange '{self._exchange_name}'")

        for method_frame, properties, body in self._channel.consume(self._queue_name):
            self._channel.basic_ack(method_frame.delivery_tag)
            yield body
