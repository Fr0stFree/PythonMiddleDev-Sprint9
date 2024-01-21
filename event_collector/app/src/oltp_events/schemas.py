import datetime as dt
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class CreateLikeSchemaIn(BaseModel):
    user_id: UUID
    movie_id: UUID
    score: Literal[0, 10]


class CreateLikeSchemaOut(CreateLikeSchemaIn):
    pass


class RemoveLikeSchemaIn(BaseModel):
    user_id: UUID
    movie_id: UUID


class GetMovieLikesSchemaOut(BaseModel):
    movie_id: UUID
    likes: list[UUID]
    dislikes: list[UUID]


class GetMovieRatingSchemaOut(BaseModel):
    movie_id: UUID
    average_score: float


class CreateReviewSchemaIn(BaseModel):
    movie_id: UUID
    author_id: UUID
    text: str = Field(..., min_length=1, max_length=1000)
    has_spoiler: bool
    attitude: Literal["positive", "neutral", "negative"]


class CreateReviewSchemaOut(CreateReviewSchemaIn):
    created_at: dt.datetime
    likes: list[UUID]
    dislikes: list[UUID]


class GetReviewsSchemaIn(BaseModel):
    movie_id: UUID = None
    author_id: UUID = None
    has_spoiler: bool = None
    attitude: Literal["positive", "neutral", "negative"] = None
    limit: int = 10


class GetReviewsSchemaOut(BaseModel):
    amount: int
    reviews: list[CreateReviewSchemaOut]


class LikeReviewSchemaIn(BaseModel):
    movie_id: UUID
    author_id: UUID
    user_id: UUID
    score: Literal[0, 10]


class LikeReviewSchemaOut(LikeReviewSchemaIn):
    pass


class RemoveLikeReviewSchemaIn(BaseModel):
    movie_id: UUID
    author_id: UUID
    user_id: UUID


class CreateBookmarkSchemaIn(BaseModel):
    user_id: UUID
    movie_id: UUID


class CreateBookmarkSchemaOut(CreateBookmarkSchemaIn):
    pass


class GetUserBookmarksSchemaOut(BaseModel):
    user_id: UUID
    bookmarks: list[UUID]


class RemoveBookmarkSchemaIn(BaseModel):
    user_id: UUID
    movie_id: UUID
