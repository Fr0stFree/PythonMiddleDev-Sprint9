from abc import ABC, abstractmethod


class AbstractBroker(ABC):
    @abstractmethod
    def send(self, messages: list[str]) -> None:
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass
