import asyncio
from typing import Any, Self

from schemas import UserEvent
from asyncio.queues import Queue


class EventProducer:
    def __init__(self, max_batch_size: int = 5):
        self._message_broker: Any
        self._queue: Queue[UserEvent] = asyncio.Queue()
        self._batch: list[UserEvent] = []
        self._max_batch_size = max_batch_size

    def add_message_broker(self, message_broker: Any) -> Self:
        self._message_broker = message_broker
        return self

    async def on_event(self, event: UserEvent) -> None:
        await self._queue.put(event)

    async def start(self) -> None:
        asyncio.ensure_future(self._produce())

    async def shutdown(self) -> None:
        await self._queue.join()
        await self._flush()

    async def _produce(self) -> None:
        while True:
            event = await self._queue.get()
            self._batch.append(event)
            if len(self._batch) >= self._max_batch_size:
                await self._flush()

            self._queue.task_done()

    async def _flush(self) -> None:
        # await self.message_broker.send_batch(self.batch)
        print('flushing')
        self._batch.clear()
