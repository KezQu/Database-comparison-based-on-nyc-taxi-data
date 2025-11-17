import pandas as pd
import pytest

from src.database_fixture_factory import DatabaseFixtureFactory


@pytest.fixture
def LoadDataIntoDatabase() -> None:
    data_parquet_path: str = DatabaseFixtureFactory.GetDatasetPath()
    loader_handle = DatabaseFixtureFactory.GetDataLoaderFunction()

    df = pd.read_parquet(data_parquet_path)  # type: ignore
    df = df.head(10000)
    print(f"Loaded {len(df)} rows from {data_parquet_path}")
    loader_handle(DatabaseFixtureFactory.GetDatabaseHandle(), df)


def test_create_data(LoadDataIntoDatabase) -> None:
    assert True
