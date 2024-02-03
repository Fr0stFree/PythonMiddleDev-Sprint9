import json
import logging
from typing import Callable

from aiohttp.web import Request, Response, middleware
from aiohttp.web_exceptions import HTTPBadRequest, HTTPForbidden, HTTPUnauthorized
from jose import jwt
from jose.exceptions import JWTError
from pydantic import ValidationError

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


def get_auth_middleware(scheme: str, decode_key: str, decode_algorithm: str) -> Callable:

    @middleware
    async def _auth_middleware(request: Request, handler: Callable) -> Response:
        token = request.headers.get("Authorization")
        if not token:
            logger.warning("Unauthorized request")
            raise HTTPUnauthorized(
                body=json.dumps({"status": "error", "detail": "No token provided"}),
                content_type="application/json",
            )

        if not token.startswith(scheme):
            logger.warning("Request with invalid token scheme %s", token)
            raise HTTPForbidden(
                body=json.dumps({"status": "error", "detail": "Invalid token"}),
                content_type="application/json",
            )

        token = token.removeprefix(scheme).strip()

        try:
            payload = jwt.decode(token, decode_key, algorithms=[decode_algorithm])
        except JWTError:
            logger.warning("Request with invalid token %s", token)
            raise HTTPForbidden(
                body=json.dumps({"status": "error", "detail": "Invalid token"}),
                content_type="application/json",
            )
        logger.info("Authenticated user %s", payload)
        setattr(request, "user", payload)
        return await handler(request)

    return _auth_middleware
