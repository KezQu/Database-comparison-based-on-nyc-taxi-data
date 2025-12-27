import logging
import typing
from itertools import product

import pandas as pd
import pytest
from pytest_benchmark.fixture import BenchmarkFixture
from redis.commands.search.query import Query
from sqlalchemy import Delete
from test_queries import (
    DELETE_QUERIES_TEST_LIST,
    SELECT_QUERIES_TEST_LIST,
    UPDATE_QUERIES_TEST_LIST,
)

from src.database_fixture_factory import DatabaseFixtureFactory
from src.framework.crud_handlers import (
    AbstractCRUDHandler,
    OrmCRUDHandler,
    RedisCRUDHandler,
)


@pytest.fixture
def SetupDatabaseContainer() -> typing.Generator[None, None, None]:
    DatabaseFixtureFactory.SetupDatabase()
    yield
    DatabaseFixtureFactory.TeardownDatabase()


def LoadRecordsToDatabase(
    records_count: int,
) -> None:
    data_parquet_path: str = DatabaseFixtureFactory.GetDatasetPath()
    loader_handle = DatabaseFixtureFactory.GetDataLoaderFunction()

    df = pd.read_parquet(data_parquet_path)  # type: ignore
    df = df.head(records_count)
    logging.info(f"Loaded {len(df)} rows from {data_parquet_path}")
    loader_handle(DatabaseFixtureFactory.GetDatabaseHandle(), df)


def GetCRUDHandler() -> AbstractCRUDHandler:
    return DatabaseFixtureFactory.ChooseBasedOnDatabaseType(
        RedisCRUDHandler(
            DatabaseFixtureFactory.GetDatabaseHandle().GetDatabaseEngine()
        ),
        OrmCRUDHandler(
            DatabaseFixtureFactory.GetDatabaseHandle().GetDatabaseEngine()
        ),
    )


RECORDS_COUNTS_TEST_LIST = [100]


@pytest.mark.skip
@pytest.mark.parametrize("records_count", RECORDS_COUNTS_TEST_LIST)
def test_create_records(
    SetupDatabaseContainer: None,
    benchmark: BenchmarkFixture,
    records_count: int,
) -> None:
    benchmark.pedantic(
        target=LoadRecordsToDatabase,
        args=(records_count,),
        teardown=DatabaseFixtureFactory.GetDatabaseHandle().FlushDatabase(),
    )


@pytest.mark.skip
@pytest.mark.parametrize(
    "records_count, read_selector",
    list(product(RECORDS_COUNTS_TEST_LIST, SELECT_QUERIES_TEST_LIST)),
)
def test_read_records_with_filter(
    SetupDatabaseContainer: None,
    benchmark: BenchmarkFixture,
    records_count: int,
    read_selector: typing.Any,
) -> None:
    LoadRecordsToDatabase(records_count)
    crud_handler: AbstractCRUDHandler = GetCRUDHandler()
    select_query = DatabaseFixtureFactory.ChooseBasedOnDatabaseType(
        *read_selector
    )
    benchmark(crud_handler.read, select_query)
    print(crud_handler.read(select_query))


@pytest.mark.skip
@pytest.mark.parametrize(
    "records_count, update_selector",
    list(product(RECORDS_COUNTS_TEST_LIST, UPDATE_QUERIES_TEST_LIST)),
)
def test_update_all_records(
    SetupDatabaseContainer: None,
    benchmark: BenchmarkFixture,
    records_count: int,
    update_selector: tuple[typing.Any, typing.Any],
) -> None:
    LoadRecordsToDatabase(records_count)
    crud_handler: AbstractCRUDHandler = GetCRUDHandler()
    update_query, update_values = (
        DatabaseFixtureFactory.ChooseBasedOnDatabaseType(*update_selector)
    )

    benchmark.pedantic(
        target=crud_handler.update,
        args=(update_query, update_values),
        rounds=1,
    )


# @pytest.mark.skip
@pytest.mark.parametrize(
    "records_count, delete_selector",
    list(product(RECORDS_COUNTS_TEST_LIST, DELETE_QUERIES_TEST_LIST)),
)
def test_delete_all_trips(
    SetupDatabaseContainer: None,
    benchmark: BenchmarkFixture,
    records_count: int,
    delete_selector: tuple[tuple[str, Query], Delete],
) -> None:
    crud_handler: AbstractCRUDHandler = GetCRUDHandler()
    delete_query = DatabaseFixtureFactory.ChooseBasedOnDatabaseType(
        *delete_selector
    )
    benchmark.pedantic(
        target=crud_handler.delete,
        args=(delete_query,),
        setup=lambda: LoadRecordsToDatabase(records_count),
        rounds=1,
    )
