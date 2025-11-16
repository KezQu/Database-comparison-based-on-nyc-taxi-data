import argparse
import os
import subprocess

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
        "--teardown-database",
        action="store_true",
        help="Teardown the database after tests",
    )
    args = parser.parse_args()

    docker_compose_file = os.path.join(
        os.path.dirname(__file__),
        f"{args.database.value.lower()}",
        "compose.yml",
    )

    subprocess.run(
        ["docker", "compose", "-f", docker_compose_file, "up", "-d"], check=True
    )

    try:
        DatabaseFixtureFactory.SetDatabaseType(args.database)
        DatabaseFixtureFactory.SetDatasetPath(args.parquet)

        pytest.main(args=["test/performance_tests.py", "-s"])
    finally:
        if args.teardown_database:
            subprocess.run(
                ["docker", "compose", "-f", docker_compose_file, "down", "-v"],
                check=True,
            )


if __name__ == "__main__":
    main()
