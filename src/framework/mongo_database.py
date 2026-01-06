import logging
import os
import time
import typing
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient

from .abstract_database import AbstractDatabase


class MongoDatabase(AbstractDatabase):
    __database_engine: typing.Optional[MongoClient] = None
    __database_name: typing.Optional[str] = None

    @classmethod
    def GetDatabaseName(cls) -> str:
        if cls.__database_name:
            return cls.__database_name
        cls.GetDatabaseEngine()
        return cls.__database_name  # type: ignore

    @classmethod
    def GetDatabaseEngine(cls) -> MongoClient:
        if cls.__database_engine:
            return cls.__database_engine

        database_env_path = Path(os.getcwd() + "/mongo/.env")
        load_dotenv(dotenv_path=database_env_path)
        database_username = os.getenv("MONGO_INITDB_ROOT_USERNAME")
        database_password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
        cls.__database_name = os.getenv("MONGO_INITDB_DATABASE")

        if (
            database_username is None
            or database_password is None
            or cls.__database_name is None
        ):
            raise EnvironmentError(
                f"Database credentials are not set in {database_env_path}."
            )

        DATABASE_URL = f"mongodb://{database_username}:{database_password}@127.0.0.1:27017/"

        logging.debug(DATABASE_URL)
        cls.__database_engine = MongoClient(DATABASE_URL)
        cls.__WaitForDatabaseReady()
        return cls.__database_engine

    @classmethod
    def FlushDatabase(cls) -> None:
        client = cls.GetDatabaseEngine()
        if cls.__database_name:
            client.drop_database(cls.__database_name)

    @classmethod
    def Reset(cls) -> None:
        cls.__database_engine = None
        cls.__database_name = None

    @classmethod
    def __WaitForDatabaseReady(cls) -> None:
        client = cls.__database_engine
        for attempt in range(10):
            try:
                client.admin.command("ping")
                logging.debug("MongoDB database is ready.")
                return
            except Exception:
                logging.warning(
                    f"Waiting for MongoDB database to be ready...{attempt + 1}/10"
                )
                time.sleep(1)
