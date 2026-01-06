import logging
import time
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

from src.database_fixture_factory import DatabaseFixtureFactory, DatabaseType
from src.framework.crud_handlers import (
    AbstractCRUDHandler,
    MongoCRUDHandler,
    OrmCRUDHandler,
    RedisCRUDHandler,
)


@pytest.fixture
def SetupDatabaseContainer() -> typing.Generator[None, None, None]:
    DatabaseFixtureFactory.SetupDatabase()
    database_type = DatabaseFixtureFactory.GetDatabaseType()
    if database_type == DatabaseType.MSSQL:
        logging.info("MSSQL needs couple seconds to start up.")
        time.sleep(10)
    DatabaseFixtureFactory.GetDatabaseHandle().GetDatabaseEngine()
    DatabaseFixtureFactory.GetDatabaseHandle().Reset()
    yield
    DatabaseFixtureFactory.TeardownDatabase()


def LoadRecordsToDatabase(
    records_count: int,
) -> None:
    data_parquet_path: str = DatabaseFixtureFactory.GetDatasetPath()
    loader_handle = DatabaseFixtureFactory.GetDataLoaderFunction()

    df = pd.read_parquet(data_parquet_path)  # type: ignore
    df = df.head(records_count)
    df.rename(
        columns={
            "Airport_fee": "airport_fee",
            "RatecodeID": "rate_code_id",
            "VendorID": "vendor_id",
            "trip_distance": "distance",
        },
        inplace=True,
    )
    logging.info(f"Loaded {len(df)} rows from {data_parquet_path}")
    loader_handle(DatabaseFixtureFactory.GetDatabaseHandle(), df)


def GetCRUDHandler() -> AbstractCRUDHandler:
    database_handle = DatabaseFixtureFactory.GetDatabaseHandle()
    return DatabaseFixtureFactory.ChooseBasedOnDatabaseType(
        redis_option=RedisCRUDHandler(database_handle.GetDatabaseEngine()),
        postgres_option=OrmCRUDHandler(database_handle.GetDatabaseEngine()),
        mssql_option=OrmCRUDHandler(database_handle.GetDatabaseEngine()),
        mongo_option=MongoCRUDHandler(
            database_handle.GetDatabaseEngine(),
            "nyc_taxi"
        ),
    )


RECORDS_COUNTS_TEST_LIST = [1000, 5000, 10000, 50000]


@pytest.mark.parametrize("records_count", RECORDS_COUNTS_TEST_LIST)
def test_create_records(
    SetupDatabaseContainer: None,
    benchmark: BenchmarkFixture,
    records_count: int,
) -> None:
    benchmark.pedantic(
        target=LoadRecordsToDatabase,
        args=(records_count,),
        teardown=lambda *_: DatabaseFixtureFactory.GetDatabaseHandle().FlushDatabase(),
        rounds=10,
    )


@pytest.mark.parametrize(
    "records_count, read_selector",
    list(product(RECORDS_COUNTS_TEST_LIST, SELECT_QUERIES_TEST_LIST)),
    ids=lambda val: str(val)
    if isinstance(val, int)
    else f"read_query{SELECT_QUERIES_TEST_LIST.index(val)}",
)
def test_read_records(
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


@pytest.mark.parametrize(
    "records_count, update_selector",
    list(product(RECORDS_COUNTS_TEST_LIST, UPDATE_QUERIES_TEST_LIST)),
    ids=lambda val: str(val)
    if isinstance(val, int)
    else f"update_query{UPDATE_QUERIES_TEST_LIST.index(val)}",
)
def test_update_records(
    SetupDatabaseContainer: None,
    benchmark: BenchmarkFixture,
    records_count: int,
    update_selector: tuple[typing.Any, typing.Any],
) -> None:
    crud_handler: AbstractCRUDHandler = GetCRUDHandler()
    update_query, update_values = (
        DatabaseFixtureFactory.ChooseBasedOnDatabaseType(*update_selector)
    )

    benchmark.pedantic(
        target=crud_handler.update,
        args=(update_query, update_values),
        setup=lambda: LoadRecordsToDatabase(records_count),
        teardown=lambda *_: DatabaseFixtureFactory.GetDatabaseHandle().FlushDatabase(),
        rounds=10,
    )


@pytest.mark.parametrize(
    "records_count, delete_selector",
    list(product(RECORDS_COUNTS_TEST_LIST, DELETE_QUERIES_TEST_LIST)),
    ids=lambda val: str(val)
    if isinstance(val, int)
    else f"delete_query{DELETE_QUERIES_TEST_LIST.index(val)}",
)
def test_delete_records(
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
        teardown=lambda *_: DatabaseFixtureFactory.GetDatabaseHandle().FlushDatabase(),
        rounds=10,
    )
