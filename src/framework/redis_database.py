import logging
import time
import typing

from redis import Redis

from .abstract_database import AbstractDatabase


class RedisDatabase(AbstractDatabase):
    __database_engine: typing.Optional[Redis] = None

    @classmethod
    def GetDatabaseEngine(cls) -> Redis:
        if cls.__database_engine:
            return cls.__database_engine

        cls.__database_engine = Redis(
            host="127.0.0.1", port=6379, decode_responses=True
        )
        cls.__WaitForDatabaseReady()
        return cls.__database_engine

    @classmethod
    def FlushDatabase(cls) -> None:
        redis_handle = cls.GetDatabaseEngine()
        redis_handle.flushdb()

    @classmethod
    def Reset(cls) -> None:
        cls.__database_engine = None

    @classmethod
    def __WaitForDatabaseReady(cls) -> None:
        engine = cls.__database_engine
        for attempt in range(10):
            try:
                engine.ping()
                logging.debug("Redis database is ready.")
                return
            except Exception:
                logging.warning(
                    f"Waiting for Redis database to be ready...{attempt + 1}/10"
                )
                time.sleep(1)
