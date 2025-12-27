import typing
from abc import ABC


class AbstractDatabase(ABC):
    @classmethod
    def GetDatabaseEngine(cls) -> typing.Any:
        raise NotImplementedError()

    @classmethod
    def WaitForDatabaseReady(cls) -> None:
        raise NotImplementedError()
