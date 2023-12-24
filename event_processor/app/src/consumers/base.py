from abc import ABC, abstractmethod
from typing import Generator, Any


class AbstractBroker(ABC):
    @abstractmethod
    def receive(self) -> Generator[Any, None, None]:
        pass

