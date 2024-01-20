import json
from http import HTTPStatus

from aiohttp.web import Request, Response, RouteTableDef

from src.oltp_events import crud
from src.oltp_events import schemas

router = RouteTableDef()


@router.post('/likes')
async def create_like(request: Request) -> Response:
    data = await request.json()
    like = await crud.create_like(request.app['db'], data)
    return Response(
        body=like.model_dump_json(),
        status=HTTPStatus.CREATED,
        content_type='application/json',
    )


@router.post('/bookmarks')
async def create_bookmark(request: Request) -> Response:
    data = await request.json()
    return Response(
        body=json.dumps({'status': 'ok'}),
        status=HTTPStatus.CREATED,
        content_type='application/json',
    )


@router.post('/reviews')
async def create_review(request: Request) -> Response:
    data = await request.json()
    return Response(
        body=json.dumps({'status': 'ok'}),
        status=HTTPStatus.CREATED,
        content_type='application/json',
    )
