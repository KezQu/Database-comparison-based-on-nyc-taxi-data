import argparse
import logging

import pytest

from src.database_fixture_factory import DatabaseFixtureFactory, DatabaseType


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
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "FATAL"],
        default="INFO",
        help="Set the logging level",
    )
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    try:
        DatabaseFixtureFactory.SetDatabaseType(args.database)
        DatabaseFixtureFactory.SetDatasetPath(args.parquet)

        pytest.main(
            args=[
                "test/performance_tests.py",
                "-s",
                "-vv",
                "--benchmark-save",
                f"performance_{args.database.value.lower()}",
            ]
        )
    except Exception:
        DatabaseFixtureFactory.TeardownDatabase()
        raise


if __name__ == "__main__":
    main()
