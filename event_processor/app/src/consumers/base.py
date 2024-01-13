from abc import ABC, abstractmethod
from typing import Generator, Union, TypeAlias


IncomingMessage: TypeAlias = Union[str, bytes]


class AbstractBroker(ABC):
    @abstractmethod
    def receive(self) -> Generator[IncomingMessage, None, None]:
        pass

    @abstractmethod
    def ack(self) -> None:
        pass
