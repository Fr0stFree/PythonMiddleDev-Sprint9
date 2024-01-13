from typing import Generator

from kafka import KafkaConsumer

from .base import AbstractBroker, IncomingMessage


class KafkaBroker(AbstractBroker):
    def __init__(self, broker_url: str, topic: str) -> None:
        self._kafka_topic = topic
        self._kafka = KafkaConsumer(
            bootstrap_servers=[broker_url],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='events-group',
        )
        self._offset_pointer = 0

    def receive(self) -> Generator[IncomingMessage, None, None]:
        print(f"Connected to kafka. Topics available: {self._kafka.topics()}")
        self._kafka.subscribe(topics=[self._kafka_topic])
        print(f"Subscribed to topic: {self._kafka_topic}")
        self._kafka.subscription()

        for message in self._kafka:
            self._offset_pointer = message.offset
            yield message.value

    def ack(self) -> None:
        self._kafka.commit()

