import asyncio
from enum import StrEnum
from typing import Sequence

from motor import motor_asyncio as ma


class MongoCollections(StrEnum):
    LIKES = "likes"
    REVIEWS = "reviews"
    BOOKMARKS = "bookmarks"


async def connect_to_mongo(username: str, password: str, host: str, port: int, db_name: str) -> ma.AsyncIOMotorDatabase:
    client = ma.AsyncIOMotorClient(f"mongodb://{username}:{password}@{host}:{port}", uuidRepresentation="standard")
    db = client[db_name]
    return db


async def create_collections(db: ma.AsyncIOMotorDatabase, collections: Sequence[str]) -> None:
    collections_to_create = set(collections) - set(await db.list_collection_names())
    if not collections_to_create:
        print(f'Collections {", ".join(collections)} already exist')
        return

    await asyncio.gather(*[db.create_collection(collection) for collection in collections_to_create])
    print(f'Collections {", ".join(collections_to_create)} created successfully')
