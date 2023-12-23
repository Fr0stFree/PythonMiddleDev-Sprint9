from enum import Enum

from pydantic import BaseModel


class EventType(Enum):
    MOVIE_LIKED = 'like'
    MOVIE_REVIEWED = 'review'
    MOVIE_BOOKMARKED = 'favorite'
    MOVIE_WATCHED = 'watch'


class UserEvent(BaseModel):
    user_id: int
    event_type: EventType
    timestamp: int
    movie_id: int
