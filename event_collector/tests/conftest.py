from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestClient
from aiohttp.web import Application
from faker import Faker

from src.handlers import CreateEventHandler


@pytest.fixture(scope="function")
def faker():
    return Faker()


@pytest.fixture(scope="function")
def app():
    return Application()


@pytest_asyncio.fixture(scope="function")
async def client(aiohttp_client, app) -> TestClient:
    handler = CreateEventHandler(callback=AsyncMock())
    app.add_routes([web.post('/event', handler)])
    return await aiohttp_client(app)
