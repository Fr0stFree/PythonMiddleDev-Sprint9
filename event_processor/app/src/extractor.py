from typing import Generator, Any

from .consumers import AbstractBroker


class EventExtractor:
    def __init__(self, message_broker: AbstractBroker) -> None:
        self._broker = message_broker

    def start(self) -> Generator[Any, None, None]:
        yield from self._broker.receive()
