import typing
from abc import ABC


class AbstractDatabase(ABC):
    @classmethod
    def GetDatabaseEngine(cls) -> typing.Any:
        raise NotImplementedError()

    @classmethod
    def FlushDatabase(cls) -> None:
        raise NotImplementedError()

    @classmethod
    def Reset(cls) -> None:
        raise NotImplementedError()
