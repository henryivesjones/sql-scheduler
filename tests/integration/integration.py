import asyncio
import os
import subprocess
import sys
from typing import Set, Union

import asyncpg

INTEGRATION_DB_DSN = f"postgres://postgres:postgres@localhost:{os.environ['INTEGRATION_PG_PORT']}/postgres"

_RESET_SCHEMAS = """
DROP SCHEMA IF EXISTS prod CASCADE;
CREATE SCHEMA prod;
DROP SCHEMA IF EXISTS dev CASCADE;
CREATE SCHEMA dev;
"""


async def _reset_schemas():
    conn = await asyncpg.connect(dsn=INTEGRATION_DB_DSN)
    await conn.execute(_RESET_SCHEMAS)
    await conn.close()


async def _assert_tables_exists(tables: Set[str]) -> Union[str, None]:
    """Returns None if all provided tables exist"""
    if len(tables) == 0:
        return None
    query = f"""
SELECT table_schema || '.' || table_name as table
FROM information_schema.tables
WHERE {" OR ".join([f"(table_schema = '{schema}' AND table_name = '{table}')" for schema, table in [table.split(".") for table in tables]])}
;
    """
    conn = await asyncpg.connect(dsn=INTEGRATION_DB_DSN)
    results = {row["table"] for row in await conn.fetch(query)}
    await conn.close()
    tables_not_found = set(tables).difference(results)
    if len(tables_not_found) == 0:
        return None
    return f"Did not find tables {tables_not_found} in database."


async def _assert_tables_dont_exist(tables: Set[str]) -> Union[str, None]:
    """Returns None if none of provided tables exist"""
    if len(tables) == 0:
        return None
    query = f"""
SELECT table_schema || '.' || table_name as table
FROM information_schema.tables
WHERE {" OR ".join([f"(table_schema = '{schema}' AND table_name = '{table}')" for schema, table in [table.split(".") for table in tables]])}
;
    """
    conn = await asyncpg.connect(dsn=INTEGRATION_DB_DSN)
    results = {row["table"] for row in await conn.fetch(query)}
    await conn.close()
    if len(results) == 0:
        return None
    return f"Found tables which should not have existed {tables} in database."


async def _assert_table_length(table: str, length: int) -> Union[str, None]:
    """Returns None if table length matches provided length"""
    query = f"""
SELECT COUNT(1) as count
FROM {table}
;
    """
    conn = await asyncpg.connect(dsn=INTEGRATION_DB_DSN)
    results = await conn.fetch(query)
    if results[0]["count"] == length:
        return None
    return f'Row count for {table} ({results[0]["count"]}) did not match expected ({length})'


def _run_sql_scheduler_command(args: list):
    result = subprocess.run(args=["sql-scheduler", *args], stdout=subprocess.DEVNULL)


ALL_TABLES = {"table_a", "table_b", "table_c", "table_d"}
SCHEMAS = {"prod", "dev"}
ALL_TABLES_AND_SCHEMAS = {
    f"{schema}.{table}" for schema in SCHEMAS for table in ALL_TABLES
}

TEST_CASES = [
    {
        "args": [["--prod"]],
        "exists": {"prod.table_a", "prod.table_b", "prod.table_c", "prod.table_d"},
        "table_lengths": [
            ("prod.table_a", 11),
            ("prod.table_b", 3),
            ("prod.table_c", 5),
            ("prod.table_d", 0),
        ],
    },
    {
        "args": [["--dev"]],
        "exists": {"dev.table_a", "dev.table_b", "dev.table_c", "dev.table_d"},
        "table_lengths": [
            ("dev.table_a", 11),
            ("dev.table_b", 3),
            ("dev.table_c", 5),
            ("dev.table_d", 0),
        ],
    },
    {
        "args": [["--prod", "-t", "prod.table_a"]],
        "exists": {"prod.table_a"},
        "table_lengths": [
            ("prod.table_a", 11),
        ],
    },
    {
        "args": [["--prod", "-t", "prod.table_c", "--dependencies"]],
        "exists": {"prod.table_a", "prod.table_c"},
        "table_lengths": [
            ("prod.table_a", 11),
            ("prod.table_c", 5),
        ],
    },
    {
        "args": [["--prod"], ["--dev", "-t", "prod.table_c"]],
        "exists": {
            "prod.table_a",
            "prod.table_b",
            "prod.table_c",
            "prod.table_d",
            "dev.table_c",
        },
        "table_lengths": [
            ("prod.table_a", 11),
            ("prod.table_b", 3),
            ("prod.table_c", 5),
            ("dev.table_c", 5),
        ],
    },
    {
        "args": [["--prod"], ["--dev", "-t", "prod.table_c", "--dependencies"]],
        "exists": {
            "prod.table_a",
            "prod.table_b",
            "prod.table_c",
            "prod.table_d",
            "dev.table_c",
            "dev.table_a",
        },
        "table_lengths": [
            ("prod.table_a", 11),
            ("prod.table_b", 3),
            ("prod.table_c", 5),
            ("dev.table_c", 5),
            ("dev.table_a", 11),
        ],
    },
    {
        "args": [
            [
                "--prod",
                "-t",
                "prod.table_d",
                "--start",
                "2023-01-01 00:00:00",
                "--end",
                "2023-01-02 23:59:59",
            ]
        ],
        "exists": {"prod.table_d"},
        "table_lengths": [("prod.table_d", 6)],
    },
    {
        "args": [
            [
                "--prod",
                "-t",
                "prod.table_d",
                "--start",
                "2023-01-01 00:00:00",
                "--end",
                "2023-01-02 23:59:59",
            ],
            [
                "--prod",
                "-t",
                "prod.table_d",
                "--refill",
                "--start",
                "2023-01-03 00:00:00",
                "--end",
                "2023-01-04 23:59:59",
            ],
        ],
        "exists": {"prod.table_d"},
        "table_lengths": [("prod.table_d", 3)],
    },
]


async def main():
    failures = []
    for test_case in TEST_CASES:
        print(f'Running test case {test_case["args"]}')
        await _reset_schemas()

        for args in test_case["args"]:
            _run_sql_scheduler_command(args)

        tables_exist = await _assert_tables_exists(test_case["exists"])
        if tables_exist is not None:
            failures.append((test_case["args"], tables_exist))
            continue

        tables_dont_exist = await _assert_tables_dont_exist(
            ALL_TABLES_AND_SCHEMAS - test_case["exists"]
        )
        if tables_dont_exist is not None:
            failures.append((test_case["args"], tables_dont_exist))
            continue

        for table, length in test_case["table_lengths"]:
            table_length = await _assert_table_length(table, length)
            if table_length is not None:
                failures.append((test_case["args"], table_length))

    if len(failures) == 0:
        print(f"All {len(TEST_CASES)} test cases passed.")
        return
    print("There were test failures:")
    for args, failure in failures:
        print(f"{args} | {failure}")
    sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
