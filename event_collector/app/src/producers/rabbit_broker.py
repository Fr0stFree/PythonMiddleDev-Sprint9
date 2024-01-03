from pika import BlockingConnection, URLParameters
from pika.exchange_type import ExchangeType

from .base import AbstractBroker


class RabbitBroker(AbstractBroker):
    def __init__(self, user: str, password: str, host: str, port: int, exchange_name: str, queue_name: str) -> None:
        self._queue_name = queue_name
        self._exchange_name = exchange_name
        self._connection = BlockingConnection(URLParameters(f"amqp://{user}:{password}@{host}:{port}"))
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=exchange_name, exchange_type=ExchangeType.direct, durable=True)
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._channel.queue_bind(exchange=exchange_name, queue=queue_name)
        print(
            f"Connected to rabbit. Ready to send messages to queue '{self._queue_name}' through exchange '{self._exchange_name}'")

    def send(self, message: str) -> None:
        self._channel.basic_publish(
            exchange=self._exchange_name,
            routing_key=self._queue_name,
            body=message,
        )

    def is_connected(self) -> bool:
        return self._connection.is_open

    def disconnect(self) -> None:
        self._connection.close()
