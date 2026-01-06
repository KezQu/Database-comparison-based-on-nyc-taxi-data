import concurrent.futures
import logging
import os
import threading
import time
import typing
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session

from .abstract_database import AbstractDatabase
from .models import BaseOrmType


class MssqlDatabase(AbstractDatabase):
    __database_engine: typing.Optional[Engine] = None

    @classmethod
    def GetDatabaseEngine(cls) -> Engine:
        if cls.__database_engine:
            return cls.__database_engine

        database_env_path = Path(os.getcwd() + "/mssql/.env")
        load_dotenv(dotenv_path=database_env_path)
        database_password = os.getenv("MSSQL_SA_PASSWORD")
        if (
            database_password is None
        ):
            raise EnvironmentError(
                f"Database credentials are not set in {database_env_path}."
            )

        DATABASE_URL = f"mssql+pymssql://sa:{database_password}@127.0.0.1:1435/master"

        logging.debug(DATABASE_URL)
        cls.__database_engine = create_engine(DATABASE_URL)

        cls.__WaitForDatabaseReady()
        BaseOrmType.metadata.create_all(cls.__database_engine)
        return cls.__database_engine

    @classmethod
    def __RunWithTimeout(cls, func: typing.Callable, timeout: int) -> typing.Any:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            future = executor.submit(func)
            try:
                return future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                raise TimeoutError(
                    f"Function {func.__name__} timed out after {timeout} seconds"
                )
        finally:
            executor.shutdown(wait=False)


    @classmethod
    def FlushDatabase(cls) -> None:
        orm_engine = cls.GetDatabaseEngine()
        with Session(orm_engine) as session:
            with session.begin():
                for table in reversed(BaseOrmType.metadata.sorted_tables):
                    session.execute(text(f"DELETE FROM {table.name}"))

    @classmethod
    def Reset(cls) -> None:
        cls.__database_engine.dispose()
        cls.__database_engine = None

    @classmethod
    def __WaitForDatabaseReady(cls) -> None:
        engine = cls.__database_engine
        if engine is None:
            return

        def check_ready():
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))

        for attempt in range(50):
            try:
                cls.__RunWithTimeout(check_ready, timeout=5)
                logging.debug("MSSQL database is ready.")
                return
            except Exception as e:
                logging.warning(
                    f"Waiting for MSSQL database to be ready...{attempt + 1}/50. Error: {e}"
                )
                time.sleep(1)
        raise RuntimeError("MSSQL database failed to become ready.")
