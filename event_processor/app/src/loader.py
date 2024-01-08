from clickhouse_driver import Client


class EventLoader:
    def __init__(self, host: str, port: int) -> None:
        self._client = Client(host=host, port=port)

    def init_database(self) -> None:
        self._create_events_database()
        self._create_clicks_table()
        self._create_page_views_table()
        self._create_movie_viewings_table()
        self._create_movie_quality_switches_table()
        self._create_filters_applications_table()

    def _create_events_database(self) -> None:
        self._client.execute("CREATE DATABASE IF NOT EXISTS events")

    def load_events(self, events: dict[str, list[dict]]) -> None:
        for event_name, event_values in events.items():
            self._client.execute(f"INSERT INTO events.{event_name} VALUES", event_values)
            print(f"Successfully loaded {len(event_values)} {event_name} events")

    def _create_clicks_table(self) -> None:
        statement = """
        CREATE TABLE IF NOT EXISTS events.clicks (
            user_id UUID,
            happened_at DateTime,
            element_id UUID,
            element_type String,
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(happened_at)
        ORDER BY (happened_at, user_id)
        """
        self._client.execute(statement)

    def _create_page_views_table(self) -> None:
        statement = """
        CREATE TABLE IF NOT EXISTS events.page_views (
            user_id UUID,
            happened_at DateTime,
            duration Float32,
            page_type String,
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(happened_at)
        ORDER BY (happened_at, user_id)
        """
        self._client.execute(statement)

    def _create_movie_quality_switches_table(self) -> None:
        statement = """
        CREATE TABLE IF NOT EXISTS events.movie_quality_switches (
            user_id UUID,
            happened_at DateTime,
            movie_id UUID,
            before String,
            after String,
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(happened_at)
        ORDER BY (happened_at, user_id)
        """
        self._client.execute(statement)

    def _create_movie_viewings_table(self) -> None:
        statement = """
        CREATE TABLE IF NOT EXISTS events.movie_viewings (
            user_id UUID,
            happened_at DateTime,
            movie_id UUID,
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(happened_at)
        ORDER BY (happened_at, user_id)
        """
        self._client.execute(statement)

    def _create_filters_applications_table(self) -> None:
        statement = """
        CREATE TABLE IF NOT EXISTS events.filters_applications (
            user_id UUID,
            happened_at DateTime,
            query String,
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(happened_at)
        ORDER BY (happened_at, user_id)
        """
        self._client.execute(statement)
