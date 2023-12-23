import asyncio
import json

from schemas import UserEvent
from asyncio.queues import Queue
from kafka import KafkaProducer


class EventRecorder:
    FLUSH_DELAY = 3
    MAX_BATCH_SIZE = 5

    def __init__(self, broker_url: str, topic: str) -> None:
        self._kafka: KafkaProducer = KafkaProducer(
            bootstrap_servers=[broker_url],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: str(x).encode('utf-8')
        )
        self._kafka_topic = topic
        self._queue: Queue[UserEvent] = asyncio.Queue()
        self._batch = []

    async def on_event(self, event: UserEvent) -> None:
        await self._queue.put(event)

    async def start(self) -> None:
        if not self._kafka.bootstrap_connected():
            raise RuntimeError(f"Connection to kafka server is not established")

        asyncio.ensure_future(self._recording())

    async def shutdown(self) -> None:
        await self._queue.join()
        self._flush_events()
        await self._kafka.close()

    async def _recording(self) -> None:
        while True:
            event = await self._queue.get()
            await self._process_event(event)
            self._queue.task_done()

    async def _process_event(self, event: UserEvent) -> None:
        if len(self._batch) == 0:
            self._flash_task = asyncio.get_running_loop().call_later(self.FLUSH_DELAY, self._flush_events)

        self._batch.append(event)

        if len(self._batch) >= self.MAX_BATCH_SIZE:
            self._flash_task.cancel()
            self._flush_events()

    def _flush_events(self) -> None:
        self._kafka.send(self._kafka_topic, [event.model_dump(mode='json') for event in self._batch])
        self._batch.clear()
