import typing

from redis import Redis

from .abstract_database import AbstractDatabase


class RedisDatabase(AbstractDatabase):
    __RedisDatabaseHandle: typing.Optional[Redis] = None

    @classmethod
    def GetDatabaseHandle(cls) -> Redis:
        if cls.__RedisDatabaseHandle:
            return cls.__RedisDatabaseHandle

        cls.__RedisDatabaseHandle = Redis(
            host="127.0.0.1", port=6379, decode_responses=True
        )

        return cls.__RedisDatabaseHandle
