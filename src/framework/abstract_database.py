import typing
from abc import ABC


class AbstractDatabase(ABC):
    @classmethod
    def GetDatabaseHandle(cls) -> typing.Any:
        raise NotImplementedError()

    @classmethod
    def WaitForDatabaseReady(cls) -> None:
        raise NotImplementedError()
