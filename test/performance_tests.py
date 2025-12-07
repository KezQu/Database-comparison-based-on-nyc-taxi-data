import logging

import pandas as pd
import pytest

from src.database_fixture_factory import DatabaseFixtureFactory


def LoadRecordsToDatabase(
    records_count: int,
) -> None:
    data_parquet_path: str = DatabaseFixtureFactory.GetDatasetPath()
    loader_handle = DatabaseFixtureFactory.GetDataLoaderFunction()

    df = pd.read_parquet(data_parquet_path)  # type: ignore
    df = df.head(records_count)
    logging.info(f"Loaded {len(df)} rows from {data_parquet_path}")
    loader_handle(DatabaseFixtureFactory.GetDatabaseHandle(), df)


@pytest.mark.parametrize("records_count", [1000, 5000, 10000])
def test_create_data(benchmark, records_count: int) -> None:
    benchmark(LoadRecordsToDatabase, records_count)
