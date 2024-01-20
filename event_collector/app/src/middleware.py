import json
from typing import Callable

from aiohttp.web import Request, Response
from aiohttp.web import middleware
from pydantic import ValidationError
from aiohttp.web_exceptions import HTTPBadRequest


@middleware
async def error_handler(request: Request, handler: Callable) -> Response:
    try:
        return await handler(request)
    except ValidationError as error:
        raise HTTPBadRequest(
            body=json.dumps({'status': 'error', 'detail': {error['loc'][0]: error['msg'] for error in error.errors()}}),
            content_type='application/json',
        )
