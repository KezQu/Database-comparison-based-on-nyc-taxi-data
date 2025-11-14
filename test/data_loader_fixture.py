import os

import pandas as pd
import pytest


@pytest.fixture
def DataLoaderFixture():
    print(os.getcwd())
    df = pd.read_parquet(
        os.path.join(os.getcwd(), "data/yellow_tripdata_2025-01.parquet")
    )

    return df
