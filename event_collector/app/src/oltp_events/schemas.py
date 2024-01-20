import datetime as dt
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class CreateLikeSchemaIn(BaseModel):
    user_id: UUID
    movie_id: UUID


class CreateLikeSchemaOut(BaseModel):
    user_id: UUID
    movie_id: UUID
    created_at: dt.datetime


class CreateReviewSchemaIn(BaseModel):
    movie_id: UUID
    author_id: UUID
    text: str = Field(..., min_length=1, max_length=1000)
    has_spoilers: bool
    attitude: Literal['positive', 'neutral', 'negative']


class CreateBookmarkSchema(BaseModel):
    user_id: UUID
    movie_id: UUID
