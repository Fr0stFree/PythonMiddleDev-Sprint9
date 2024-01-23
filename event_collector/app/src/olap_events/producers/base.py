from abc import ABC, abstractmethod


class AbstractBroker(ABC):
    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def send(self, message: bytes) -> None:
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        pass
