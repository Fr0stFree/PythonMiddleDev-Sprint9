from typing import Literal

from settings import Settings
from src.consumers import KafkaBroker, RabbitBroker, RedisBroker, AbstractBroker
from src.extractor import EventExtractor
from src.loader import EventLoader
from src.transformer import EventTransformer


def get_broker(broker: Literal['kafka', 'rabbitmq', 'redis']) -> AbstractBroker:
    if broker == 'kafka':
        return KafkaBroker(broker_url=settings.kafka_url, topic=settings.kafka_topic)
    elif broker == 'rabbitmq':
        return RabbitBroker(host=settings.rabbitmq_host, port=settings.rabbitmq_port,
                            user=settings.rabbitmq_user, password=settings.rabbitmq_password,
                            exchange_name=settings.rabbitmq_exchange, queue_name=settings.rabbitmq_queue_name)
    elif broker == 'redis':
        return RedisBroker(host=settings.redis_host, port=settings.redis_port, channel_name=settings.redis_channel)


if __name__ == '__main__':
    settings = Settings()
    extractor = EventExtractor(message_broker=get_broker(settings.selected_broker))
    transformer = EventTransformer()
    loader = EventLoader(settings.clickhouse_host, settings.clickhouse_port)

    loader.init_database()

    for event_stream in extractor.start():
        events = transformer.serialize_stream(event_stream)
        loader.load_events(events)
        extractor.ack()
