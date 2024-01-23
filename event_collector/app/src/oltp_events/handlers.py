import json
from http import HTTPStatus
from uuid import UUID

from aiohttp.web import Request, Response, RouteTableDef

from src.oltp_events import crud

router = RouteTableDef()


@router.post("/likes")
async def like_movie(request: Request) -> Response:
    data = await request.json()
    like = await crud.like_movie(request.app["db"], data)
    return Response(
        body=like.model_dump_json(),
        status=HTTPStatus.CREATED,
        content_type="application/json",
    )


@router.delete("/likes")
async def remove_like(request: Request) -> Response:
    data = await request.json()
    await crud.remove_like(request.app["db"], data)
    return Response(status=HTTPStatus.NO_CONTENT)


@router.get("/likes/{movie_id}")
async def get_movie_likes(request: Request) -> Response:
    movie_id = UUID(request.match_info["movie_id"])
    score = await crud.get_movie_likes(request.app["db"], movie_id)
    return Response(
        body=score.model_dump_json(),
        status=HTTPStatus.OK,
        content_type="application/json",
    )


@router.get("/likes/{movie_id}/rating")
async def get_movie_rating(request: Request) -> Response:
    movie_id = UUID(request.match_info["movie_id"])
    score = await crud.get_movie_rating(request.app["db"], movie_id)
    return Response(
        body=score.model_dump_json(),
        status=HTTPStatus.OK,
        content_type="application/json",
    )


@router.post("/bookmarks")
async def create_bookmark(request: Request) -> Response:
    data = await request.json()
    bookmark = await crud.create_bookmark(request.app["db"], data)
    return Response(
        body=bookmark.model_dump_json(),
        status=HTTPStatus.CREATED,
        content_type="application/json",
    )


@router.get("/bookmarks/{user_id}")
async def get_user_bookmarks(request: Request) -> Response:
    user_id = UUID(request.match_info["user_id"])
    bookmarks = await crud.get_user_bookmarks(request.app["db"], user_id)
    return Response(
        body=bookmarks.model_dump_json(),
        status=HTTPStatus.OK,
        content_type="application/json",
    )


@router.delete("/bookmarks")
async def remove_bookmark(request: Request) -> Response:
    data = await request.json()
    await crud.remove_bookmark(request.app["db"], data)
    return Response(status=HTTPStatus.NO_CONTENT)


@router.get("/bookmarks/{user_id}")
async def get_user_bookmarks(request: Request) -> Response:
    user_id = UUID(request.match_info["user_id"])
    bookmarks = await crud.get_user_bookmarks(request.app["db"], user_id)
    return Response(
        body=json.dumps(bookmarks),
        status=HTTPStatus.OK,
        content_type="application/json",
    )


@router.post("/reviews")
async def create_review(request: Request) -> Response:
    data = await request.json()
    review = await crud.create_review(request.app["db"], data)
    return Response(
        body=review.model_dump_json(),
        status=HTTPStatus.CREATED,
        content_type="application/json",
    )


@router.get("/reviews")
async def get_reviews(request: Request) -> Response:
    reviews = await crud.get_reviews(request.app["db"], request.query)
    return Response(
        body=reviews.model_dump_json(),
        status=HTTPStatus.OK,
        content_type="application/json",
    )


@router.post("/reviews/likes")
async def like_review(request: Request) -> Response:
    data = await request.json()
    review = await crud.like_review(request.app["db"], data)
    return Response(
        body=review.model_dump_json(),
        status=HTTPStatus.CREATED,
        content_type="application/json",
    )
