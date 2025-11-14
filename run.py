import argparse
import enum
import os
import subprocess

import pandas as pd

from src.abstract_database import AbstractDatabase
from src.postgres_database import PostgresDatabase
from src.redis_database import RedisDatabase


class DatabaseType(enum.StrEnum):
    POSTGRES = enum.auto()
    REDIS = enum.auto()


def SetupDatabase(db_type: DatabaseType) -> AbstractDatabase:
    match db_type:
        case DatabaseType.POSTGRES:
            database_handle = PostgresDatabase()
        case DatabaseType.REDIS:
            database_handle = RedisDatabase()
        case _:
            raise ValueError(f"Unsupported database type: {db_type}")

    docker_compose_file = os.path.join(
        os.path.dirname(__file__),
        f"{db_type.value.lower()}",
        "compose.yml",
    )
    subprocess.run(
        ["docker", "compose", "-f", docker_compose_file, "up", "-d"], check=True
    )

    return database_handle


def LoadDataIntoDatabase(
    database_handle: AbstractDatabase, data_parquet_path: str
) -> None:
    df = pd.read_parquet(data_parquet_path)
    print(f"Loaded {len(df)} rows from {data_parquet_path}")
    raise NotImplementedError("WARNING: Loading data not implemented yet.")


def main():
    parser = argparse.ArgumentParser(
        description="Database performance tests runner."
    )
    parser.add_argument(
        "--database",
        type=lambda db_type: DatabaseType[db_type.upper()],
        required=True,
        help="Database type to run",
    )
    parser.add_argument(
        "--parquet",
        type=str,
        required=True,
        help="Path to the parquet file with data to load",
    )

    args = parser.parse_args()

    database_handle = SetupDatabase(args.database)
    LoadDataIntoDatabase(database_handle, args.parquet)


if __name__ == "__main__":
    main()
