import argparse
import logging
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
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "FATAL"],
        default="INFO",
        help="Set the logging level",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=5,
        help="Number of benchmark rounds to run",
    )
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

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

        pytest.main(
            args=[
                "test/performance_tests.py",
                "-s",
                "--benchmark-min-rounds",
                str(args.rounds),
            ]
        )
    finally:
        if args.teardown_database:
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    docker_compose_file,
                    "down",
                    "-v",
                ],
                check=True,
            )


if __name__ == "__main__":
    main()
