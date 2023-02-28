import asyncio
import os
import sys

import asyncpg

INTEGRATION_DB_DSN = f"postgres://postgres:postgres@localhost:{os.environ['INTEGRATION_PG_PORT']}/postgres"

_DDL = [
    """
DROP SCHEMA IF EXISTS raw_d CASCADE;
CREATE SCHEMA raw_d;
    """,
    """
CREATE TABLE raw_d.raw_table_a (
    column_a INT,
    column_b VARCHAR(12),
    column_c INT
);
""",
    """
CREATE TABLE raw_d.raw_table_b (
    column_a INT,
    column_d INT
);
""",
    """
CREATE TABLE raw_d.raw_table_c (
    column_d INT,
    column_e VARCHAR(12)
);
""",
    """
CREATE TABLE raw_d.raw_table_d (
    column_dt TIMESTAMP,
    column_a INT
);
""",
]

_INSERT = [
    """
INSERT INTO raw_d.raw_table_a VALUES
(1, 'a', 1),
(1, 'a', 2),
(1, 'b', 1),
(1, 'b', 2),

(2, 'a', 1),
(2, 'a', 2),
(2, 'b', 1),
(2, 'b', 2),

(3, 'a', 1),
(3, 'a', 2),
(3, 'a', 3)
;
    """,
    """
INSERT INTO raw_d.raw_table_b VALUES
(1, 4),
(2, 5),
(3, 6)
;
    """,
    """
INSERT INTO raw_d.raw_table_c VALUES
(4, 10),
(4, 11),
(4, 12),
(4, 13),

(4, 20),
(4, 21),
(4, 22),
(4, 23),

(4, 30),
(4, 31),
(4, 32),
(4, 33)
;
    """,
    """
INSERT INTO raw_d.raw_table_d VALUES
('2023-01-01 00:00:00', 1),
('2023-01-02 00:00:00', 1),
('2023-01-03 00:00:00', 1),
('2023-01-04 00:00:00', 1),
('2023-01-01 00:00:00', 2),
('2023-01-02 00:00:00', 2),
('2023-01-03 00:00:00', 2),
('2023-01-01 00:00:00', 3),
('2023-01-02 00:00:00', 3)
;
    """,
]

_INSERT_VERIFICATION = """
SELECT COUNT(1)
FROM raw_d.raw_table_a a
INNER JOIN raw_d.raw_table_b b on a.column_a = b.column_a
INNER JOIN raw_d.raw_table_c c on b.column_d = c.column_d
INNER JOIN raw_d.raw_table_d d on a.column_a = d.column_a
;
"""


async def main():
    print("Waiting for PG to be ready...")
    connection_attempts = 0
    while True:
        try:
            conn = await asyncpg.connect(dsn=INTEGRATION_DB_DSN)
            break
        except:
            connection_attempts += 1
            if connection_attempts >= 20:
                print("unable to connect to PG after 20 attempts")
                sys.exit(1)

            await asyncio.sleep(1)
    print("Connected to PG. Setting up DB.")
    async with conn.transaction():
        for ddl in _DDL:
            await conn.execute(ddl)
        for insert in _INSERT:
            await conn.execute(insert)
    print("Done setting up db.")
    result = await conn.fetch(_INSERT_VERIFICATION)
    if len(result) != 1:
        print("Failed setup data validation (FATAL)")
        sys.exit(1)
    if result[0]["count"] != 192:
        print(f"Failed setup data validation (ROWCOUNT) ({result[0]['count']})")
        sys.exit(1)
    print("Setup data passed validation.")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
