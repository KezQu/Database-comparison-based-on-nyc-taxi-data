import logging
import os
import time
import typing
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session

from .abstract_database import AbstractDatabase
from .models import BaseOrmType


class PostgresDatabase(AbstractDatabase):
    __database_engine: typing.Optional[Engine] = None

    @classmethod
    def GetDatabaseEngine(cls) -> Engine:
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

        logging.debug(DATABASE_URL)
        cls.__database_engine = create_engine(DATABASE_URL)
        cls.__WaitForDatabaseReady()
        BaseOrmType.metadata.create_all(cls.__database_engine)
        return cls.__database_engine

    @classmethod
    def FlushDatabase(cls) -> None:
        orm_engine = cls.GetDatabaseEngine()
        with Session(orm_engine) as session:
            with session.begin():
                session.execute(
                    text(
                        "TRUNCATE TABLE {} RESTART IDENTITY CASCADE;".format(
                            ", ".join(
                                table.name
                                for table in BaseOrmType.metadata.sorted_tables
                            )
                        )
                    )
                )

    @classmethod
    def Reset(cls) -> None:
        cls.__database_engine = None

    @classmethod
    def __WaitForDatabaseReady(cls) -> None:
        engine = cls.__database_engine
        for attempt in range(10):
            try:
                with engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                logging.debug("PostgreSQL database is ready.")
                return
            except Exception:
                logging.warning(
                    f"Waiting for PostgreSQL database to be ready...{attempt + 1}/10"
                )
                time.sleep(1)
