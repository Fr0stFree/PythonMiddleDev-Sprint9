from settings import Settings
from extractor import EventExtractor
from transformer import EventTransformer
from loader import EventLoader


if __name__ == '__main__':
    settings = Settings()
    extractor = EventExtractor(settings.kafka_url, settings.kafka_topic)
    transformer = EventTransformer()
    loader = EventLoader(settings.clickhouse_url)

    for events in extractor.start():
        transformed_events = transformer.transform(events)
        loader.load(transformed_events)

