import enum
import os
import subprocess
import sys
import typing

import pandas as pd

from .framework.abstract_database import AbstractDatabase
from .framework.mongo_database import MongoDatabase
from .framework.mssql_database import MssqlDatabase
from .framework.postgres_database import PostgresDatabase
from .framework.redis_database import RedisDatabase
from .nyc_data_loaders import (
    LoadNycTaxiDataToMongoDatabase,
    LoadNycTaxiDataToRedisDatabase,
    LoadNycTaxiDataToSqlDatabase,
)


class DatabaseType(enum.StrEnum):
    POSTGRES = enum.auto()
    REDIS = enum.auto()
    MSSQL = enum.auto()
    MONGO = enum.auto()
    UNKNOWN = enum.auto()


class DatabaseFixtureFactory:
    database_type: DatabaseType = DatabaseType.UNKNOWN
    dataset_path: str = ""
    docker_compose_file: str = ""

    @classmethod
    def SetDatabaseType(cls, db_type: DatabaseType) -> None:
        cls.database_type = db_type
        cls.docker_compose_file = os.path.join(
            os.path.dirname(os.path.abspath(sys.argv[0])),
            f"{cls.database_type.value.lower()}",
            "compose.yml",
        )

    @classmethod
    def GetDatabaseType(cls) -> DatabaseType:
        return cls.database_type

    @classmethod
    def SetDatasetPath(cls, dataset_path: str) -> None:
        cls.dataset_path = dataset_path

    @classmethod
    def GetDatasetPath(cls) -> str:
        return cls.dataset_path

    @classmethod
    def SetupDatabase(cls) -> None:
        subprocess.run(
            ["docker", "compose", "-f", cls.docker_compose_file, "up", "-d"],
            check=True,
        )

    @classmethod
    def TeardownDatabase(cls) -> None:
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                cls.docker_compose_file,
                "down",
                "-v",
            ],
            check=True,
        )
        import time
        time.sleep(2)
        cls.GetDatabaseHandle().Reset()

    @classmethod
    def ChooseBasedOnDatabaseType(
        cls,
        redis_option: typing.Any,
        postgres_option: typing.Any,
        mssql_option: typing.Any,
        mongo_option: typing.Any,
    ) -> typing.Any:
        match cls.database_type:
            case DatabaseType.POSTGRES:
                return postgres_option
            case DatabaseType.REDIS:
                return redis_option
            case DatabaseType.MSSQL:
                return mssql_option
            case DatabaseType.MONGO:
                return mongo_option
            case _:
                raise ValueError(
                    f"Unsupported database type: {cls.database_type}"
                )

    @classmethod
    def GetDatabaseHandle(cls) -> AbstractDatabase:
        return cls.ChooseBasedOnDatabaseType(
            redis_option=RedisDatabase(),
            postgres_option=PostgresDatabase(),
            mssql_option=MssqlDatabase(),
            mongo_option=MongoDatabase(),
        )

    @classmethod
    def GetDataLoaderFunction(
        cls,
    ) -> typing.Callable[[AbstractDatabase, pd.DataFrame], None]:
        return cls.ChooseBasedOnDatabaseType(
            redis_option=LoadNycTaxiDataToRedisDatabase,
            postgres_option=LoadNycTaxiDataToSqlDatabase,
            mssql_option=LoadNycTaxiDataToSqlDatabase,
            mongo_option=LoadNycTaxiDataToMongoDatabase,
        )
