import time
from http import HTTPStatus

import pytest

from event_collector.app.src.olap_events.schemas import EventType


@pytest.mark.asyncio
class TestSendClickEvent:

    @pytest.mark.parametrize("element_type", ["button", "link", "image", "video", "input", "other"])
    async def test_send_valid_click_event(self, client, faker, element_type):
        request_data = {
            "type": EventType.CLICK.value,
            "user_id": faker.uuid4(),
            "happened_at": int(time.time()),
            "element_type": element_type,
            "element_id": faker.uuid4(),
        }

        response = await client.post("/event", json=request_data)
        body = await response.json()

        assert body == {"status": "ok"}
        assert response.status == HTTPStatus.ACCEPTED

    async def test_send_click_event_with_invalid_element_type(self, client, faker):
        request_data = {
            "type": EventType.CLICK.value,
            "user_id": faker.uuid4(),
            "happened_at": int(time.time()),
            "element_type": "invalid",
            "element_id": faker.uuid4(),
        }

        response = await client.post("/event", json=request_data)
        body = await response.json()

        assert body["status"] == "error"
        assert "detail" in body
        assert response.status == HTTPStatus.BAD_REQUEST


@pytest.mark.asyncio
class TestSendPageViewedEvent:

    @pytest.mark.parametrize("page_type", ["home", "home", "account", "movie", "categories", "actor", "advertisement", "other"])
    async def test_send_valid_page_viewed_event(self, client, faker, page_type):
        request_data = {
            "type": EventType.PAGE_VIEWED.value,
            "user_id": faker.uuid4(),
            "happened_at": int(time.time()),
            "page_type": page_type,
            "duration": faker.pyfloat(min_value=60.00, max_value=200.00),
        }

        response = await client.post("/event", json=request_data)
        body = await response.json()

        assert body == {"status": "ok"}
        assert response.status == HTTPStatus.ACCEPTED

    async def test_send_page_viewed_event_with_invalid_page_type(self, client, faker):
        request_data = {
            "type": EventType.PAGE_VIEWED.value,
            "user_id": faker.uuid4(),
            "happened_at": int(time.time()),
            "page_type": "invalid",
            "duration": faker.pyfloat(min_value=60.00, max_value=200.00),
        }

        response = await client.post("/event", json=request_data)
        body = await response.json()

        assert body["status"] == "error"
        assert "detail" in body
        assert response.status == HTTPStatus.BAD_REQUEST


@pytest.mark.asyncio
class TestSendMovieQualityChangedEvent:

    @pytest.mark.parametrize("quality", ["SD", "HD", "FULL_HD"])
    async def test_send_valid_movie_quality_changed_event(self, client, faker, quality):
        request_data = {
            "type": EventType.MOVIE_QUALITY_CHANGED.value,
            "user_id": faker.uuid4(),
            "happened_at": int(time.time()),
            "movie_id": faker.uuid4(),
            "quality_before": "SD",
            "quality_after": quality,
        }

        response = await client.post("/event", json=request_data)
        body = await response.json()

        assert body == {"status": "ok"}
        assert response.status == HTTPStatus.ACCEPTED

    async def test_send_movie_quality_changed_event_with_invalid_quality(self, client, faker):
        request_data = {
            "type": EventType.MOVIE_QUALITY_CHANGED.value,
            "user_id": faker.uuid4(),
            "happened_at": int(time.time()),
            "movie_id": faker.uuid4(),
            "quality_before": "SD",
            "quality_after": "invalid",
        }

        response = await client.post("/event", json=request_data)
        body = await response.json()

        assert body["status"] == "error"
        assert "detail" in body
        assert response.status == HTTPStatus.BAD_REQUEST


@pytest.mark.asyncio
class TestSendMovieWatchedEvent:

    async def test_send_valid_movie_watched_event(self, client, faker):
        request_data = {
            "type": EventType.MOVIE_WATCHED.value,
            "user_id": faker.uuid4(),
            "happened_at": int(time.time()),
            "movie_id": faker.uuid4(),
        }

        response = await client.post("/event", json=request_data)
        body = await response.json()

        assert body == {"status": "ok"}
        assert response.status == HTTPStatus.ACCEPTED


@pytest.mark.asyncio
class TestSendFiltersAppliedEvent:

    async def test_send_valid_filters_applied_event(self, client, faker):
        request_data = {
            "type": EventType.FILTERS_APPLIED.value,
            "user_id": faker.uuid4(),
            "happened_at": int(time.time()),
            "filters_query": "genre=action&year_gt=2010&year_lt=2020&language=en",
        }

        response = await client.post("/event", json=request_data)
        body = await response.json()

        assert body == {"status": "ok"}
        assert response.status == HTTPStatus.ACCEPTED
