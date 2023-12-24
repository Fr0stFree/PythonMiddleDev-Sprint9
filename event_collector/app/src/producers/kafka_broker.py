import json

from kafka import KafkaProducer

from .base import AbstractBroker


class KafkaBroker(AbstractBroker):
    def __init__(self, broker_url: str, topic: str) -> None:
        self._kafka: KafkaProducer = KafkaProducer(
            bootstrap_servers=[broker_url],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: str(x).encode('utf-8')
        )
        self._kafka_topic = topic
        print(f"Connected to kafka. Ready to send messages to topic '{self._kafka_topic}'")

    def send(self, messages: list[dict]) -> None:
        self._kafka.send(self._kafka_topic, value=messages)

    def is_connected(self) -> bool:
        return self._kafka.bootstrap_connected()

    def disconnect(self) -> None:
        self._kafka.close()
