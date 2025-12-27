import logging
import typing
from itertools import product

import pandas as pd
import pytest
from pytest_benchmark.fixture import BenchmarkFixture
from redis.commands.search.query import Query
from sqlalchemy import and_, delete, update
from test_queries import SELECT_QUERIES_TEST_LIST

import src.framework.models as models
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


# @pytest.mark.skip
@pytest.mark.parametrize(
    "records_count, query_selector",
    list(product(RECORDS_COUNTS_TEST_LIST, SELECT_QUERIES_TEST_LIST)),
)
def test_read_records_with_filter(
    SetupDatabaseContainer: None,
    benchmark: BenchmarkFixture,
    records_count: int,
    query_selector: typing.Any,
) -> None:
    LoadRecordsToDatabase(records_count)
    crud_handler: AbstractCRUDHandler = GetCRUDHandler()
    select_query = DatabaseFixtureFactory.ChooseBasedOnDatabaseType(
        *query_selector
    )
    benchmark(crud_handler.read, select_query)
    print(crud_handler.read(select_query))


@pytest.mark.skip
@pytest.mark.parametrize("records_count", [200])
def test_update_all_records(
    SetupDatabaseContainer: None,
    benchmark: BenchmarkFixture,
    records_count: int,
) -> None:
    LoadRecordsToDatabase(records_count)
    crud_handler: AbstractCRUDHandler = GetCRUDHandler()
    update_query = DatabaseFixtureFactory.ChooseBasedOnDatabaseType(
        ("idx:trip", Query("@RatecodeID:[2 6]")),
        update(models.Payment).where(
            and_(
                models.Payment.rate_code_id >= 2,
                models.Payment.rate_code_id <= 6,
            )
        ),
    )
    update_values = dict(
        DatabaseFixtureFactory.ChooseBasedOnDatabaseType(
            {"RatecodeID": 1}, {"rate_code_id": 1}
        )
    )

    benchmark.pedantic(
        target=crud_handler.update,
        args=(update_query, update_values),
        rounds=1,
    )


@pytest.mark.skip
@pytest.mark.parametrize("records_count", [10])
def test_delete_all_trips(
    SetupDatabaseContainer: None,
    benchmark: BenchmarkFixture,
    records_count: int,
) -> None:
    crud_handler: AbstractCRUDHandler = GetCRUDHandler()
    delete_query = DatabaseFixtureFactory.ChooseBasedOnDatabaseType(
        ("idx:trip", Query("*")), delete(models.Trip)
    )
    benchmark.pedantic(
        target=crud_handler.delete,
        args=(delete_query,),
        setup=lambda: LoadRecordsToDatabase(records_count),
        rounds=1,
    )
