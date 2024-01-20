import asyncio

from protocol.events_pb2 import EventStream
from src.olap_events.producers import AbstractBroker
from src.olap_events.schemas import EventSchema


class EventRecorder:
    FLUSH_DELAY = 3
    MAX_BATCH_SIZE = 5

    def __init__(self, message_broker: AbstractBroker) -> None:
        self._broker = message_broker
        self._queue = asyncio.Queue()
        self._batch = []

    async def on_event(self, event: bytes) -> None:
        await self._queue.put(event)

    async def start(self) -> None:
        await self._broker.connect()
        asyncio.ensure_future(self._run())

    async def shutdown(self) -> None:
        await self._queue.join()
        self._flush_events()
        await self._broker.disconnect()

    async def _run(self) -> None:
        while True:
            event = await self._queue.get()
            await self._process_event(event)
            self._queue.task_done()

    async def _process_event(self, event: EventSchema) -> None:
        if len(self._batch) == 0:
            self._flash_task = asyncio.get_running_loop().call_later(self.FLUSH_DELAY, self._flush_events)

        self._batch.append(event)

        if len(self._batch) >= self.MAX_BATCH_SIZE:
            self._flash_task.cancel()
            self._flush_events()

    def _flush_events(self) -> None:
        stream = EventStream(events=[event.to_proto() for event in self._batch]).SerializeToString()
        asyncio.create_task(self._broker.send(stream))
        print(f"Successfully sent {len(self._batch)} event(s) to broker")
        self._batch.clear()
