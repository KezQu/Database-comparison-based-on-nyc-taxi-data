import enum
import typing

import pandas as pd

from .framework.abstract_database import AbstractDatabase
from .framework.postgres_database import PostgresDatabase
from .framework.redis_database import RedisDatabase
from .nyc_data_loaders import (
    LoadNycTaxiDataToRedisDatabase,
    LoadNycTaxiDataToSqlDatabase,
)


class DatabaseType(enum.StrEnum):
    POSTGRES = enum.auto()
    REDIS = enum.auto()
    UNKNOWN = enum.auto()


class DatabaseFixtureFactory:
    database_type: DatabaseType = DatabaseType.UNKNOWN
    dataset_path: str = ""

    @classmethod
    def SetDatabaseType(cls, db_type: DatabaseType) -> None:
        cls.database_type = db_type

    @classmethod
    def SetDatasetPath(cls, dataset_path: str) -> None:
        cls.dataset_path = dataset_path

    @classmethod
    def GetDatasetPath(cls) -> str:
        return cls.dataset_path

    @classmethod
    def GetDatabaseHandle(cls) -> AbstractDatabase:
        match cls.database_type:
            case DatabaseType.POSTGRES:
                return PostgresDatabase()
            case DatabaseType.REDIS:
                return RedisDatabase()
            case _:
                raise ValueError(
                    f"Unsupported database type: {cls.database_type}"
                )

    @classmethod
    def GetDataLoaderFunction(
        cls,
    ) -> typing.Callable[[AbstractDatabase, pd.DataFrame], None]:
        match cls.database_type:
            case DatabaseType.POSTGRES:
                return LoadNycTaxiDataToSqlDatabase
            case DatabaseType.REDIS:
                return LoadNycTaxiDataToRedisDatabase
            case _:
                raise ValueError(
                    f"Unsupported database type: {cls.database_type}"
                )
