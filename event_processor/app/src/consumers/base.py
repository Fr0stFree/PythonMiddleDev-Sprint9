from abc import ABC, abstractmethod
from typing import Generator, Union


class AbstractBroker(ABC):
    @abstractmethod
    def receive(self) -> Generator[Union[str, bytes], None, None]:
        pass

