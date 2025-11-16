import os
import typing
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine

from .abstract_database import AbstractDatabase
from .models import BaseOrmType


class PostgresDatabase(AbstractDatabase):
    __database_engine: typing.Optional[Engine] = None

    @classmethod
    def GetDatabaseHandle(cls) -> typing.Any:
        if cls.__database_engine:
            return cls.__database_engine

        database_env_path = Path(os.getcwd() + "/postgres/.env")
        load_dotenv(dotenv_path=database_env_path)
        database_username = os.getenv("POSTGRES_USER")
        database_password = os.getenv("POSTGRES_PASSWORD")
        database_name = os.getenv("POSTGRES_DB")
        if (
            database_username is None
            or database_password is None
            or database_name is None
        ):
            raise EnvironmentError(
                f"Database credentials are not set in {database_env_path}."
            )
        DATABASE_URL = f"postgresql://{database_username}:{database_password}@127.0.0.1:5432/{database_name}"

        print(DATABASE_URL)
        cls.__database_engine = create_engine(DATABASE_URL)
        BaseOrmType.metadata.create_all(cls.__database_engine)
        return cls.__database_engine
