import json
from http import HTTPStatus
from typing import Callable

from aiohttp.web import Request, Response

from src.schemas import UserEvent


class CreateEventHandler:
    def __init__(self, callback: Callable) -> None:
        self._event_callback = callback

    async def __call__(self, request: Request) -> Response:
        try:
            data = await request.text()
            event = UserEvent.model_validate_json(data)
        except Exception as error:
            return Response(status=HTTPStatus.BAD_REQUEST, text=str(error))
        await self._event_callback(event)
        return Response(text=json.dumps({'status': 'ok'}), headers={'Content-Type': 'application/json'})
