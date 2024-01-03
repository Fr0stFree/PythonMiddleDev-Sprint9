from typing import Generator

from google.protobuf.json_format import MessageToDict

from protocol.events_pb2 import EventStream
from .consumers import AbstractBroker


class EventExtractor:
    def __init__(self, message_broker: AbstractBroker) -> None:
        self._broker = message_broker

    def start(self) -> Generator[dict, None, None]:
        for message in self._broker.receive():
            stream = EventStream.FromString(message)
            for event in stream.events:
                result = MessageToDict(event)
                print(result)
                yield result
