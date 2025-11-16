import typing
from abc import ABC


class AbstractDatabase(ABC):
    @classmethod
    def GetDatabaseHandle(cls) -> typing.Any:
        print("wrong handler")
        raise NotImplementedError()
