import pandas as pd
import pytest

from src.postgres_database import PostgresDatabase


@pytest.fixture
def PostgresDatabaseFixture():
    postgres_engine = PostgresDatabase().GetDatabaseEngine()
    postgres_engine

    df = pd.read_parquet(
        "/mnt/c/Users/jmiku/Downloads/yellow_tripdata_2025-01.parquet"
    )

    return
