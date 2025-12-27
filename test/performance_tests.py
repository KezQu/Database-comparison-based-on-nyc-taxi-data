import logging
import typing

import pandas as pd
import pytest
from redis.commands.search.query import Query
from sqlalchemy import select

import src.framework.models as models
from src.database_fixture_factory import DatabaseFixtureFactory
from src.framework.crud_handlers import (
    AbstractCRUDHandler,
    OrmCRUDHandler,
    RedisCRUDHandler,
)


def LoadRecordsToDatabase(
    records_count: int,
) -> None:
    data_parquet_path: str = DatabaseFixtureFactory.GetDatasetPath()
    loader_handle = DatabaseFixtureFactory.GetDataLoaderFunction()

    df = pd.read_parquet(data_parquet_path)  # type: ignore
    df = df.head(records_count)
    logging.info(f"Loaded {len(df)} rows from {data_parquet_path}")
    loader_handle(DatabaseFixtureFactory.GetDatabaseHandle(), df)


@pytest.fixture
def GetCRUDHandler() -> AbstractCRUDHandler:
    return DatabaseFixtureFactory.ChooseBasedOnDatabaseType(
        RedisCRUDHandler(
            DatabaseFixtureFactory.GetDatabaseHandle().GetDatabaseEngine()
        ),
        OrmCRUDHandler(
            DatabaseFixtureFactory.GetDatabaseHandle().GetDatabaseEngine()
        ),
    )


@pytest.mark.parametrize("records_count", [1000, 5000, 10000])
def test_create_records(
    benchmark: typing.Callable[..., None], records_count: int
) -> None:
    benchmark(LoadRecordsToDatabase, records_count)


@pytest.mark.skip
@pytest.mark.parametrize("records_count", [1000, 5000, 10000])
def test_read_all_records(
    GetCRUDHandler: AbstractCRUDHandler,
    benchmark: typing.Callable[..., None],
    records_count: int,
) -> None:
    LoadRecordsToDatabase(records_count)
    crud_handler: AbstractCRUDHandler = GetCRUDHandler
    read_all_query = DatabaseFixtureFactory.ChooseBasedOnDatabaseType(
        ("idx:trip", Query("*")), select(models.Trip)
    )
    benchmark(crud_handler.read, read_all_query)
