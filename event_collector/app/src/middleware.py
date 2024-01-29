import json
from typing import Callable
import logging

from aiohttp.web import Request, Response, middleware
from pydantic import ValidationError
from aiohttp.web_exceptions import HTTPBadRequest


logger = logging.getLogger(__name__)


@middleware
async def error_handler(request: Request, handler: Callable) -> Response:
    try:
        return await handler(request)
    except ValidationError as error:
        logger.exception("Validation error")
        raise HTTPBadRequest(
            body=json.dumps({"status": "error", "detail": {error["loc"][0]: error["msg"] for error in error.errors()}}),
            content_type="application/json",
        )
