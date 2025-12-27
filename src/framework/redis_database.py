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

        return cls.__database_engine

    @classmethod
    def FlushDatabase(cls) -> None:
        redis_handle = cls.GetDatabaseEngine()
        redis_handle.flushdb()

    @classmethod
    def Reset(cls) -> None:
        cls.__database_engine = None
