class EventLoader:
    def __init__(self, clickhouse_url: str) -> None:
        self._clickhouse_url = clickhouse_url

    def load(self, events) -> None:
        pass
