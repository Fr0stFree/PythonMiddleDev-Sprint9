import json
from typing import Generator

from kafka import KafkaConsumer


class EventExtractor:
    def __init__(self, kafka_url: str, kafka_topic: str) -> None:
        self._kafka_topic = kafka_topic
        self._kafka = KafkaConsumer(
            bootstrap_servers=[kafka_url],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='events-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def start(self) -> Generator[list[dict], None, None]:
        print(f"Connected to kafka. Topics available: {self._kafka.topics()}")
        self._kafka.subscribe(topics=[self._kafka_topic])
        self._kafka.subscription()

        for message in self._kafka:
            yield message.value
