import logging
from aiohttp import web

from logstash import LogstashHandler


class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = ...
        return True


def setup_logger(app, logstash_host: str = None, logstash_port: int = None) -> None:
    logging.basicConfig(level=logging.INFO)
    if logstash_host and logstash_port:
        app.logger.addHandler(LogstashHandler(logstash_host, logstash_port, version=1))
    else:
        app.logger.addHandler(logging.StreamHandler())

    app.logger.addFilter(RequestIdFilter())
    app.logger.setLevel(logging.INFO)

