from enum import Enum

from pydantic import BaseModel


class EventType(Enum):
    MOVIE_LIKED = 'like'
    MOVIE_REVIEWED = 'review'
    MOVIE_BOOKMARKED = 'favorite'
    MOVIE_WATCHED = 'watch'
    MOUSE_CLICKED = 'click'
    PAGE_VIEWED = 'view'


class UserEvent(BaseModel):
    user_id: int
    event_type: EventType
    event_time: str
    movie_id: int
