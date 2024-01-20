import datetime as dt
import json

from aiohttp.web_exceptions import HTTPConflict
from motor import motor_asyncio as ma

from src.oltp_events.db import MongoCollections
from src.oltp_events import schemas


async def create_like(db: ma.AsyncIOMotorDatabase, data: str) -> schemas.CreateLikeSchemaOut:
    like = schemas.CreateLikeSchemaIn.model_validate(data)
    is_exist = await db[MongoCollections.LIKES].find_one({'user_id': like.user_id, 'movie_id': like.movie_id})
    if is_exist:
        raise HTTPConflict(
            body=json.dumps({'status': 'error', 'detail': f'User {like.user_id} already liked movie {like.movie_id}'}),
            content_type='application/json',
        )
    document = {"created_at": dt.datetime.now()}
    document.update(like.model_dump())
    await db[MongoCollections.LIKES].insert_one(document)
    return schemas.CreateLikeSchemaOut(**document)


async def create_review(db: ma.AsyncIOMotorDatabase, data: dict) -> None:
    review = schemas.CreateReviewSchemaIn.model_validate(data)
    is_exist = await db[MongoCollections.REVIEWS].find_one({'user_id': review.user_id, 'movie_id': review.movie_id})
    if is_exist:
        raise HTTPConflict(
            body=json.dumps({'status': 'error', 'detail': f'User {review.user_id} already reviewed movie {review.movie_id}'}),
            content_type='application/json',
        )
    document = {}
    document.update(review.model_dump())
    await db[MongoCollections.REVIEWS].insert_one(document)

