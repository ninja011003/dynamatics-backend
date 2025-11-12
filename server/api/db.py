from pymongo import MongoClient
from pymongo.database import Database
from fastapi import Request


def get_mongo_client(request: Request) -> MongoClient:
    return request.app.state.mongo_client


def get_db(request: Request, db_name: str = "dynamatics-backend") -> Database:
    client = get_mongo_client(request)
    return client[db_name]
