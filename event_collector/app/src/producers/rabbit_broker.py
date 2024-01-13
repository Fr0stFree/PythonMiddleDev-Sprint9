import aio_pika

from .base import AbstractBroker


class RabbitBroker(AbstractBroker):
    def __init__(self, user: str, password: str, host: str, port: int, exchange_name: str, queue_name: str) -> None:
        self._connection: aio_pika.Connection = None
        self._channel: aio_pika.Channel = None
        self._exchange: aio_pika.Exchange = None
        self._queue: aio_pika.Queue = None
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._exchange_name = exchange_name
        self._queue_name = queue_name

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(
            host=self._host,
            port=self._port,
            login=self._user,
            password=self._password,
        )
        self._channel = await self._connection.channel()
        self._exchange = await self._channel.declare_exchange(
            name=self._exchange_name,
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self._queue = await self._channel.declare_queue(
            name=self._queue_name,
            durable=True,
        )
        await self._queue.bind(self._exchange)
        print(f"Connected to rabbit."
              f"Ready to send messages to queue '{self._queue_name}' through exchange '{self._exchange_name}'")

    async def send(self, message: bytes) -> None:
        message = aio_pika.Message(body=message, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
        await self._exchange.publish(message, routing_key=self._queue.name)

    async def disconnect(self) -> None:
        await self._connection.close()
