import enum
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, PositiveInt, PositiveFloat, field_validator, ValidationError
from protocol.events_pb2 import MovieWatched, MovieQualityChanged, PageViewed, FiltersApplied, Click, BaseEventInfo, Event


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
    movie_id: UUID = None
    element_id: UUID = None
    element_type: str = None
    duration: PositiveFloat = None
    page_type: str = None
    quality_before: str = None
    quality_after: str = None
    filters_query: str = None

    @field_validator("element_type")
    def validate_element_type(cls, v, values) -> int:
        try:
            return Click.ElementType.Value(v.upper())
        except ValueError as error:
            raise ValueError(f"Element type must be one of {Click.ElementType.keys()}") from error

    @field_validator("quality_after")
    def validate_quality_after(cls, v, values) -> int:
        try:
            return MovieQualityChanged.Quality.Value(v.upper())
        except ValueError as error:
            raise ValueError(f"Quality after must be one of {MovieQualityChanged.Quality.keys()}") from error

    @field_validator("quality_before")
    def validate_quality_before(cls, v, values) -> int:
        try:
            return MovieQualityChanged.Quality.Value(v.upper())
        except ValueError as error:
            raise ValueError(f"Quality before must be one of {MovieQualityChanged.Quality.keys()}") from error

    @field_validator("page_type")
    def validate_page_type(cls, v, values) -> int:
        try:
            return PageViewed.PageType.Value(v.upper())
        except ValueError as error:
            raise ValueError(f"Page type must be one of {PageViewed.PageType.keys()}") from error

    @field_validator("movie_id")
    def validate_movie_id(cls, v, values) -> str:
        return str(v)

    @field_validator("element_id")
    def validate_element_id(cls, v, values) -> str:
        return str(v)

    @field_validator("user_id")
    def validate_user_id(cls, v, values) -> str:
        return str(v)

    def to_proto(self) -> Event:
        info = BaseEventInfo(user_id=str(self.user_id), happened_at=self.happened_at)

        match self.type:
            case EventType.CLICK:
                return Event(click=Click(info=info, element_id=str(self.element_id), element_type=self.element_type))

            case EventType.PAGE_VIEWED:
                return Event(page_viewed=PageViewed(info=info, page_type=self.page_type))

            case EventType.MOVIE_QUALITY_CHANGED:
                return Event(
                    movie_quality_changed=MovieQualityChanged(
                        info=info, movie_id=str(self.movie_id), before=self.quality_before, after=self.quality_after
                    )
                )

            case EventType.MOVIE_WATCHED:
                return Event(movie_watched=MovieWatched(info=info, movie_id=self.movie_id, duration=self.duration))

            case EventType.FILTERS_APPLIED:
                return Event(filters_applied=FiltersApplied(info=info, query=self.filters_query))
