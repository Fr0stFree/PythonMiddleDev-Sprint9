import json
from http import HTTPStatus
from typing import Callable

from aiohttp.web import Request, Response
from aiohttp.web_exceptions import HTTPBadRequest

from event_collector.app.src.olap_events.schemas import EventSchema


class CreateEventHandler:
    def __init__(self, callback: Callable) -> None:
        self._event_callback = callback

    async def __call__(self, request: Request) -> Response:
        try:
            data = await request.text()
            event = EventSchema.model_validate_json(data)
        except Exception as error:
            raise HTTPBadRequest(
                body=json.dumps({"status": "error", "detail": str(error)}),
                content_type="application/json",
            )
        await self._event_callback(event)
        return Response(
            text=json.dumps({"status": "ok"}),
            content_type="application/json",
            status=HTTPStatus.ACCEPTED,
        )
