import datetime as dt
import json
import logging
from itertools import chain
from uuid import UUID

from aiohttp.web_exceptions import HTTPConflict, HTTPNotFound
from motor import motor_asyncio as ma

from src.oltp_events import schemas
from src.oltp_events.db import MongoCollections


logger = logging.getLogger(__name__)


async def like_movie(db: ma.AsyncIOMotorDatabase, data: str) -> schemas.CreateLikeSchemaOut:
    like = schemas.CreateLikeSchemaIn.model_validate(data)
    logger.info(f"Received like: {like}. Looking for existing like from user {like.user_id} to movie {like.movie_id}")
    is_exist = await db[MongoCollections.LIKES].find_one({"user_id": like.user_id, "movie_id": like.movie_id})
    if is_exist:
        logger.info(f"User {like.user_id} already liked movie {like.movie_id}. Updating score to {like.score}")
        await db[MongoCollections.LIKES].update_one(
            {"user_id": like.user_id, "movie_id": like.movie_id},
            {"$set": {"score": like.score}},
        )
        return schemas.CreateLikeSchemaOut(user_id=like.user_id, movie_id=like.movie_id, score=like.score)
    document = like.model_dump()
    logger.info(f"User {like.user_id} did not like movie {like.movie_id}. Creating new like with score {like.score}")
    await db[MongoCollections.LIKES].insert_one(document)
    return schemas.CreateLikeSchemaOut(**document)


async def remove_like(db: ma.AsyncIOMotorDatabase, data: dict) -> None:
    like = schemas.RemoveLikeSchemaIn.model_validate(data)
    logger.info(f"Received request to remove like from user {like.user_id} to movie {like.movie_id}")
    result = await db[MongoCollections.LIKES].delete_one({"user_id": like.user_id, "movie_id": like.movie_id})
    if not result.deleted_count:
        logger.info(f"User {like.user_id} did not like movie {like.movie_id}")
        raise HTTPNotFound(
            body=json.dumps({"status": "error", "detail": f"User {like.user_id} did not like movie {like.movie_id}"}),
            content_type="application/json",
        )


async def get_movie_likes(db: ma.AsyncIOMotorDatabase, movie_id: UUID) -> schemas.GetMovieLikesSchemaOut:
    logger.info(f"Received request to get likes for movie {movie_id}")
    cursor = db[MongoCollections.LIKES].find({"movie_id": movie_id}, {"user_id": 1, "score": 1})
    documents = await cursor.to_list(length=None)
    logger.info(f"Found {len(documents)} likes for movie {movie_id}")
    return schemas.GetMovieLikesSchemaOut(
        movie_id=movie_id,
        likes=[document["user_id"] for document in documents if document["score"] == 10],
        dislikes=[document["user_id"] for document in documents if document["score"] == 0],
    )


async def get_movie_rating(db: ma.AsyncIOMotorDatabase, movie_id: UUID) -> schemas.GetMovieRatingSchemaOut:
    logger.info(f"Received request to get rating for movie {movie_id}")
    cursor = db[MongoCollections.LIKES].aggregate(
        [
            {"$match": {"movie_id": movie_id}},
            {"$group": {"_id": None, "average_score": {"$avg": "$score"}}},
        ]
    )
    results = await cursor.to_list(length=1)
    logger.info(f"Found rating {results[0]['average_score']} for movie {movie_id}")
    return schemas.GetMovieRatingSchemaOut(movie_id=movie_id, average_score=results[0]["average_score"])


async def create_review(db: ma.AsyncIOMotorDatabase, data: dict) -> schemas.CreateReviewSchemaOut:
    review = schemas.CreateReviewSchemaIn.model_validate(data)
    logger.info(f"Received review: {review}. Looking for existing review from user {review.author_id} to movie {review.movie_id}")
    is_exist = await db[MongoCollections.REVIEWS].find_one({"author_d": review.author_id, "movie_id": review.movie_id})
    if is_exist:
        logger.info(f"User {review.author_id} already reviewed movie {review.movie_id}")
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
    logger.info(f"User {review.author_id} did not review movie {review.movie_id}. Creating new review")
    await db[MongoCollections.REVIEWS].insert_one(document)
    return schemas.CreateReviewSchemaOut(**document)


async def get_reviews(db: ma.AsyncIOMotorDatabase, data: dict) -> schemas.GetReviewsSchemaOut:
    filter_params = schemas.GetReviewsSchemaIn.model_validate(data)
    logger.info(f"Received request to get reviews with filter: {filter_params}")

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
    logger.info(f"Found {len(documents)} reviews with filter: {filter_params}")
    return schemas.GetReviewsSchemaOut(amount=amount, reviews=documents)


async def like_review(db: ma.AsyncIOMotorDatabase, data: dict) -> schemas.CreateReviewSchemaOut:
    like = schemas.LikeReviewSchemaIn.model_validate(data)
    logger.info(f"Received like: {like}. Looking for existing review from user {like.author_id} to movie {like.movie_id}")
    review = await db[MongoCollections.REVIEWS].find_one({"author_id": like.author_id, "movie_id": like.movie_id})
    if not review:
        logger.info(f"User {like.author_id} did not review movie {like.movie_id}")
        raise HTTPNotFound(
            body=json.dumps(
                {"status": "error", "detail": f"User {like.author_id} did not review movie {like.movie_id}"}
            ),
            content_type="application/json",
        )
    if like.user_id in chain(review["likes"], review["dislikes"]):
        logger.info(f"User {like.user_id} already liked review from user {like.author_id} to movie {like.movie_id}")
        await db[MongoCollections.REVIEWS].update_one(
            {"author_id": like.author_id, "movie_id": like.movie_id},
            {"$pull": {"likes": like.user_id, "dislikes": like.user_id}},
        )
    await db[MongoCollections.REVIEWS].update_one(
        {"author_id": like.author_id, "movie_id": like.movie_id},
        {"$push": {"likes" if like.score == 10 else "dislikes": like.user_id}},
    )
    logger.info(f"User {like.user_id} liked review from user {like.author_id} to movie {like.movie_id}")
    review = await db[MongoCollections.REVIEWS].find_one({"author_id": like.author_id, "movie_id": like.movie_id})
    return schemas.CreateReviewSchemaOut(**review)


async def create_bookmark(db: ma.AsyncIOMotorDatabase, data: dict) -> schemas.CreateBookmarkSchemaOut:
    bookmark = schemas.CreateBookmarkSchemaIn.model_validate(data)
    logger.info(f"Received bookmark: {bookmark}. Looking for existing bookmark from user {bookmark.user_id} to movie {bookmark.movie_id}")
    is_exist = await db[MongoCollections.BOOKMARKS].find_one(
        {"user_id": bookmark.user_id, "movie_id": bookmark.movie_id}
    )
    if is_exist:
        logger.info(f"User {bookmark.user_id} already bookmarked movie {bookmark.movie_id}")
        raise HTTPConflict(
            body=json.dumps(
                {"status": "error", "detail": f"User {bookmark.user_id} already bookmarked movie {bookmark.movie_id}"}
            ),
            content_type="application/json",
        )
    document = bookmark.model_dump()
    await db[MongoCollections.BOOKMARKS].insert_one(document)
    logger.info(f"User {bookmark.user_id} did not bookmark movie {bookmark.movie_id}. Creating new bookmark")
    return schemas.CreateBookmarkSchemaOut(**document)


async def get_user_bookmarks(db: ma.AsyncIOMotorDatabase, user_id: UUID) -> schemas.GetUserBookmarksSchemaOut:
    logger.info(f"Received request to get bookmarks for user {user_id}")
    cursor = db[MongoCollections.BOOKMARKS].find({"user_id": user_id}, {"movie_id": 1})
    documents = await cursor.to_list(length=None)
    logger.info(f"Found {len(documents)} bookmarks for user {user_id}")
    return schemas.GetUserBookmarksSchemaOut(
        user_id=user_id, bookmarks=[document["movie_id"] for document in documents]
    )


async def remove_bookmark(db: ma.AsyncIOMotorDatabase, data: dict) -> None:
    bookmark = schemas.RemoveBookmarkSchemaIn.model_validate(data)
    logger.info(f"Received request to remove bookmark from user {bookmark.user_id} to movie {bookmark.movie_id}")
    result = await db[MongoCollections.BOOKMARKS].delete_one(
        {"user_id": bookmark.user_id, "movie_id": bookmark.movie_id}
    )
    if not result.deleted_count:
        logger.info(f"User {bookmark.user_id} did not bookmark movie {bookmark.movie_id}")
        raise HTTPNotFound(
            body=json.dumps(
                {"status": "error", "detail": f"User {bookmark.user_id} did not bookmark movie {bookmark.movie_id}"}
            ),
            content_type="application/json",
        )
