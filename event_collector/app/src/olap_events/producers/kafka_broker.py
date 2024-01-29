import logging

from aiokafka import AIOKafkaProducer

from .base import AbstractBroker


logger = logging.getLogger(__name__)

class KafkaBroker(AbstractBroker):
    def __init__(self, broker_url: str, topic: str) -> None:
        self._kafka: AIOKafkaProducer = None
        self._broker_url = broker_url
        self._kafka_topic = topic

    async def connect(self) -> None:
        self._kafka = AIOKafkaProducer(bootstrap_servers=self._broker_url)
        await self._kafka.start()
        logger.info(f"Connected to kafka. Ready to send messages to topic '{self._kafka_topic}'")

    async def send(self, message: bytes) -> None:
        await self._kafka.send_and_wait(topic=self._kafka_topic, value=message)

    async def disconnect(self) -> None:
        await self._kafka.stop()
