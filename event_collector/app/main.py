from aiohttp import web

from handlers import CreateEventHandler
from producer import EventProducer
from settings import Settings


async def startup(app: web.Application) -> None:
    producer.add_message_broker(None)
    await producer.start()


async def shutdown(app: web.Application) -> None:
    await producer.shutdown()


if __name__ == '__main__':
    settings = Settings()
    app = web.Application()
    producer = EventProducer()

    app.add_routes([web.post('/event', CreateEventHandler(callback=producer.on_event))])
    app.on_startup.append(startup)
    app.on_shutdown.append(shutdown)
    web.run_app(app, port=settings.app_port, host=settings.app_host)

