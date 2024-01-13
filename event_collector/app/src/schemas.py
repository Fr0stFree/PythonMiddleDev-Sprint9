from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, PositiveInt, PositiveFloat, field_validator, model_validator

from protocol.events_pb2 import MovieWatched, MovieQualityChanged, PageViewed, FiltersApplied, Click, BaseEventInfo, \
    Event


class EventType(Enum):
    CLICK = 'click'
    PAGE_VIEWED = 'page_viewed'
    MOVIE_QUALITY_CHANGED = 'movie_quality_changed'
    MOVIE_WATCHED = 'movie_watched'
    FILTERS_APPLIED = 'filters_applied'


class EventSchema(BaseModel):
    type: EventType
    user_id: UUID
    happened_at: PositiveInt
    element_id: UUID = None
    element_type: str = None
    movie_id: UUID = None
    duration: PositiveFloat = None
    page_type: str = None
    quality_before: str = None
    quality_after: str = None
    filters_query: str = None

    @classmethod
    def get_required_fields(cls, event_type: EventType) -> set["str"]:
        required_fields_map = {
            EventType.CLICK: {"user_id", "happened_at", "element_id", "element_type"},
            EventType.PAGE_VIEWED: {"user_id", "happened_at", "page_type", "duration"},
            EventType.MOVIE_QUALITY_CHANGED: {"user_id", "happened_at", "movie_id", "quality_before", "quality_after"},
            EventType.MOVIE_WATCHED: {"user_id", "happened_at", "movie_id"},
            EventType.FILTERS_APPLIED: {"user_id", "happened_at", "filters_query"},
        }
        return required_fields_map[event_type]

    @model_validator(mode="before")
    @classmethod
    def validate_type(cls, data: dict[str, Any]) -> dict[str, Any]:
        required_fields = cls.get_required_fields(EventType(data["type"]))
        if not required_fields.issubset(set(data.keys())):
            raise ValueError(f"Event type {data['type']} requires the following fields: {required_fields}")
        return data

    @field_validator("element_type")
    @classmethod
    def validate_element_type(cls, v: str) -> int:
        try:
            return Click.ElementType.Value(v.upper())
        except ValueError as error:
            raise ValueError(f"Element type must be one of {Click.ElementType.keys()}") from error

    @field_validator("quality_after")
    @classmethod
    def validate_quality_after(cls, v: str) -> int:
        try:
            return MovieQualityChanged.Quality.Value(v.upper())
        except ValueError as error:
            raise ValueError(f"Quality after must be one of {MovieQualityChanged.Quality.keys()}") from error

    @field_validator("quality_before")
    @classmethod
    def validate_quality_before(cls, v: str) -> int:
        try:
            return MovieQualityChanged.Quality.Value(v.upper())
        except ValueError as error:
            raise ValueError(f"Quality before must be one of {MovieQualityChanged.Quality.keys()}") from error

    @field_validator("page_type")
    @classmethod
    def validate_page_type(cls, v: str) -> int:
        try:
            return PageViewed.PageType.Value(v.upper())
        except ValueError as error:
            raise ValueError(f"Page type must be one of {PageViewed.PageType.keys()}") from error

    @field_validator("movie_id")
    @classmethod
    def validate_movie_id(cls, v: UUID) -> str:
        return str(v)

    @field_validator("element_id")
    @classmethod
    def validate_element_id(cls, v: UUID) -> str:
        return str(v)

    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v: UUID) -> str:
        return str(v)

    def to_proto(self) -> Event:
        info = BaseEventInfo(user_id=self.user_id, happened_at=self.happened_at)

        match self.type:
            case EventType.CLICK:
                return Event(click=Click(info=info, element_id=self.element_id, element_type=self.element_type))

            case EventType.PAGE_VIEWED:
                return Event(page_viewed=PageViewed(info=info, page_type=self.page_type))

            case EventType.MOVIE_QUALITY_CHANGED:
                return Event(
                    movie_quality_changed=MovieQualityChanged(
                        info=info, movie_id=self.movie_id, before=self.quality_before, after=self.quality_after
                    )
                )

            case EventType.MOVIE_WATCHED:
                return Event(movie_watched=MovieWatched(info=info, movie_id=self.movie_id))

            case EventType.FILTERS_APPLIED:
                return Event(filters_applied=FiltersApplied(info=info, query=self.filters_query))
