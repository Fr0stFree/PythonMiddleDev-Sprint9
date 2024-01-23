import datetime as dt
import json
from itertools import chain
from uuid import UUID

from aiohttp.web_exceptions import HTTPConflict, HTTPNotFound
from motor import motor_asyncio as ma

from src.oltp_events import schemas
from src.oltp_events.db import MongoCollections


async def like_movie(db: ma.AsyncIOMotorDatabase, data: str) -> schemas.CreateLikeSchemaOut:
    like = schemas.CreateLikeSchemaIn.model_validate(data)
    is_exist = await db[MongoCollections.LIKES].find_one({"user_id": like.user_id, "movie_id": like.movie_id})
    if is_exist:
        await db[MongoCollections.LIKES].update_one(
            {"user_id": like.user_id, "movie_id": like.movie_id},
            {"$set": {"score": like.score}},
        )
        return schemas.CreateLikeSchemaOut(user_id=like.user_id, movie_id=like.movie_id, score=like.score)
    document = like.model_dump()
    await db[MongoCollections.LIKES].insert_one(document)
    return schemas.CreateLikeSchemaOut(**document)


async def remove_like(db: ma.AsyncIOMotorDatabase, data: dict) -> None:
    like = schemas.RemoveLikeSchemaIn.model_validate(data)
    result = await db[MongoCollections.LIKES].delete_one({"user_id": like.user_id, "movie_id": like.movie_id})
    if not result.deleted_count:
        raise HTTPNotFound(
            body=json.dumps({"status": "error", "detail": f"User {like.user_id} did not like movie {like.movie_id}"}),
            content_type="application/json",
        )


async def get_movie_likes(db: ma.AsyncIOMotorDatabase, movie_id: UUID) -> schemas.GetMovieLikesSchemaOut:
    cursor = db[MongoCollections.LIKES].find({"movie_id": movie_id}, {"user_id": 1, "score": 1})
    documents = await cursor.to_list(length=None)
    return schemas.GetMovieLikesSchemaOut(
        movie_id=movie_id,
        likes=[document["user_id"] for document in documents if document["score"] == 10],
        dislikes=[document["user_id"] for document in documents if document["score"] == 0],
    )


async def get_movie_rating(db: ma.AsyncIOMotorDatabase, movie_id: UUID) -> schemas.GetMovieRatingSchemaOut:
    cursor = db[MongoCollections.LIKES].aggregate(
        [
            {"$match": {"movie_id": movie_id}},
            {"$group": {"_id": None, "average_score": {"$avg": "$score"}}},
        ]
    )
    results = await cursor.to_list(length=1)
    return schemas.GetMovieRatingSchemaOut(movie_id=movie_id, average_score=results[0]["average_score"])


async def create_review(db: ma.AsyncIOMotorDatabase, data: dict) -> schemas.CreateReviewSchemaOut:
    review = schemas.CreateReviewSchemaIn.model_validate(data)
    is_exist = await db[MongoCollections.REVIEWS].find_one({"author_d": review.author_id, "movie_id": review.movie_id})
    if is_exist:
        raise HTTPConflict(
            body=json.dumps(
                {"status": "error", "detail": f"User {review.author_id} already reviewed movie {review.movie_id}"}
            ),
            content_type="application/json",
        )
    document = {
        "created_at": dt.datetime.utcnow(),
        "updated_at": dt.datetime.utcnow(),
        "likes": [],
        "dislikes": [],
    }
    document.update(review.model_dump())
    await db[MongoCollections.REVIEWS].insert_one(document)
    return schemas.CreateReviewSchemaOut(**document)


async def get_reviews(db: ma.AsyncIOMotorDatabase, data: dict) -> schemas.GetReviewsSchemaOut:
    filter_params = schemas.GetReviewsSchemaIn.model_validate(data)

    statement = {}
    if filter_params.movie_id:
        statement["movie_id"] = filter_params.movie_id
    if filter_params.author_id:
        statement["author_id"] = filter_params.author_id
    if filter_params.has_spoiler is not None:
        statement["has_spoiler"] = filter_params.has_spoiler
    if filter_params.attitude:
        statement["attitude"] = filter_params.attitude

    cursor = db[MongoCollections.REVIEWS].find(statement)
    documents = await cursor.to_list(length=filter_params.limit)
    amount = await db[MongoCollections.REVIEWS].count_documents(statement)
    return schemas.GetReviewsSchemaOut(amount=amount, reviews=documents)


async def like_review(db: ma.AsyncIOMotorDatabase, data: dict) -> schemas.CreateReviewSchemaOut:
    like = schemas.LikeReviewSchemaIn.model_validate(data)
    review = await db[MongoCollections.REVIEWS].find_one({"author_id": like.author_id, "movie_id": like.movie_id})
    if not review:
        raise HTTPNotFound(
            body=json.dumps(
                {"status": "error", "detail": f"User {like.author_id} did not review movie {like.movie_id}"}
            ),
            content_type="application/json",
        )
    if like.user_id in chain(review["likes"], review["dislikes"]):
        await db[MongoCollections.REVIEWS].update_one(
            {"author_id": like.author_id, "movie_id": like.movie_id},
            {"$pull": {"likes": like.user_id, "dislikes": like.user_id}},
        )
    await db[MongoCollections.REVIEWS].update_one(
        {"author_id": like.author_id, "movie_id": like.movie_id},
        {"$push": {"likes" if like.score == 10 else "dislikes": like.user_id}},
    )
    review = await db[MongoCollections.REVIEWS].find_one({"author_id": like.author_id, "movie_id": like.movie_id})
    return schemas.CreateReviewSchemaOut(**review)


async def create_bookmark(db: ma.AsyncIOMotorDatabase, data: dict) -> schemas.CreateBookmarkSchemaOut:
    bookmark = schemas.CreateBookmarkSchemaIn.model_validate(data)
    is_exist = await db[MongoCollections.BOOKMARKS].find_one(
        {"user_id": bookmark.user_id, "movie_id": bookmark.movie_id}
    )
    if is_exist:
        raise HTTPConflict(
            body=json.dumps(
                {"status": "error", "detail": f"User {bookmark.user_id} already bookmarked movie {bookmark.movie_id}"}
            ),
            content_type="application/json",
        )
    document = bookmark.model_dump()
    await db[MongoCollections.BOOKMARKS].insert_one(document)
    return schemas.CreateBookmarkSchemaOut(**document)


async def get_user_bookmarks(db: ma.AsyncIOMotorDatabase, user_id: UUID) -> schemas.GetUserBookmarksSchemaOut:
    cursor = db[MongoCollections.BOOKMARKS].find({"user_id": user_id}, {"movie_id": 1})
    documents = await cursor.to_list(length=None)
    return schemas.GetUserBookmarksSchemaOut(
        user_id=user_id, bookmarks=[document["movie_id"] for document in documents]
    )


async def remove_bookmark(db: ma.AsyncIOMotorDatabase, data: dict) -> None:
    bookmark = schemas.RemoveBookmarkSchemaIn.model_validate(data)
    result = await db[MongoCollections.BOOKMARKS].delete_one(
        {"user_id": bookmark.user_id, "movie_id": bookmark.movie_id}
    )
    if not result.deleted_count:
        raise HTTPNotFound(
            body=json.dumps(
                {"status": "error", "detail": f"User {bookmark.user_id} did not bookmark movie {bookmark.movie_id}"}
            ),
            content_type="application/json",
        )
