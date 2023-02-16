import asyncio
import hashlib
import os
import re
import time
from enum import Enum
from typing import List, Literal, Set

import asyncpg

from . import _constants
from ._helpers import w_print


class SQLTaskDDLFileNotExists(Exception):
    pass


class SQLTaskInsertFileNotExists(Exception):
    pass


_FROM_JOIN_REGEXP = re.compile(
    r"""(?:from|join)\s+"?(?P<after>[\w\d]*?"?\."?[\w\d]*)"?\s*""", flags=re.IGNORECASE
)
_INSERT_REGEXP = re.compile(
    r'(?P<before>insert\s*into\s*"?)([\w\d]*)(?P<after>"?\."?[\w\d]*"?)',
    flags=re.IGNORECASE,
)
_UPDATE_REGEXP = re.compile(
    r'(?P<before>update\s*"?)([\w\d]*)(?P<after>"?\."?[\w\d]*"?)', flags=re.IGNORECASE
)

_CREATE_TABLE_REGEXP = re.compile(
    r'(?P<before>create\s*table\s*(if)?\s*(not)?\s*(exists)?\s*"?)([\w\d]*)(?P<after>"?\."?[\w\d]*"?)',
    flags=re.IGNORECASE,
)
_DROP_TABLE_REGEXP = re.compile(
    r'(?P<before>drop\s*table\s*(if)?\s*(not)?\s*(exists)?\s*"?)([\w\d]*)(?P<after>"?\."?[\w\d]*"?)',
    flags=re.IGNORECASE,
)

_DELETE_REGEXP = re.compile(
    r'(?P<before>delete\sfrom\s*"?)([\w\d]*)(?P<after>"?\."?[\w\d]*"?)',
    flags=re.IGNORECASE,
)

_GRANULARITY_TEST_REGEXP = re.compile(r"granularity:([\w, ]*)", flags=re.IGNORECASE)
_GRANULARITY_TEST = """
SELECT {columns}
FROM "{schema}"."{table}"
GROUP BY {columns}
HAVING count(1) > 1
LIMIT 1;
""".strip()

_NOT_NULL_TEST_REGEXP = re.compile(r"not_null:([\w, ]*)", flags=re.IGNORECASE)
_NOT_NULL_TEST = """
SELECT 1
FROM "{schema}"."{table}"
WHERE
{columns}
LIMIT 1;
""".strip()

_RELATIONSHIP_TEST_REGEXP = re.compile(
    r"relationship: ?([\w_]+ ?= ?[\w_]+\.[\w_]+\.[\w_]+)", flags=re.IGNORECASE
)
_RELATIONSHIP_TEST = """
SELECT 1
FROM "{schema}"."{table}" AS a
LEFT JOIN "{r_schema}"."{r_table}" AS b on a."{column}" = b."{r_column}"
WHERE b."{r_column}" IS NULL
LIMIT 1;
""".strip()


class SQLTaskStatus(Enum):
    TEST_FAILED = -3
    UPSTREAM_FAILED = -2
    FAILED = -1
    WAITING = 0
    RUNNING = 1
    SUCCESS = 2


class SQLTask:
    task_id: str
    ddl_directory: str
    insert_directory: str
    dependencies: Set[str]
    status: Literal[
        SQLTaskStatus.UPSTREAM_FAILED,
        SQLTaskStatus.FAILED,
        SQLTaskStatus.WAITING,
        SQLTaskStatus.TEST_FAILED,
        SQLTaskStatus.RUNNING,
        SQLTaskStatus.SUCCESS,
    ]
    stage: Literal["dev", "prod"]
    dev_schema: str
    dsn: str
    failed_tests: List[str]
    cache_duration: int
    cache_filename: str
    no_cache: bool

    def __init__(
        self,
        ddl_directory: str,
        insert_directory: str,
        task_id: str,
        stage: Literal["prod", "dev"],
        dev_schema: str,
        dsn: str,
        cache_duration: int,
        no_cache: bool,
    ):
        self.task_id = task_id
        self.ddl_directory = ddl_directory
        self.insert_directory = insert_directory
        self.stage = stage
        self.dev_schema = dev_schema
        self.dsn = dsn
        self.cache_duration = cache_duration
        self.cache_filename = os.path.join(
            _constants._CACHE_DIR, f"{self.task_id.lower()}.txt"
        )
        self.no_cache = no_cache

        self.dependencies = self._parse_dependencies()
        self.status = SQLTaskStatus.WAITING
        self.failed_tests = []

    def get_ddl(self) -> str:
        ddl_file_path = os.path.join(
            self.ddl_directory,
            f"{self.task_id}{_constants._TASK_FILE_ENDING}",
        )
        if not os.path.exists(ddl_file_path):
            raise SQLTaskDDLFileNotExists()
        with open(ddl_file_path, "r") as f:
            return f.read()

    def get_insert(self) -> str:
        insert_file_path = os.path.join(
            self.insert_directory,
            f"{self.task_id}{_constants._TASK_FILE_ENDING}",
        )
        if not os.path.exists(insert_file_path):
            raise SQLTaskInsertFileNotExists()
        with open(insert_file_path, "r") as f:
            return f.read()

    def _parse_dependencies(self) -> Set[str]:
        return set(
            [table.lower() for table in _FROM_JOIN_REGEXP.findall(self.get_insert())]
        )

    def remove_second_class_dependencies(
        self, first_class_dependencies: Set[str]
    ) -> None:
        self.dependencies.intersection_update(first_class_dependencies)

    async def run_granularity_test(self, columns: List[str]):
        conn = await asyncpg.connect(dsn=self.dsn)
        schema, table = self.task_id.lower().split(".")
        if self.stage == _constants._STAGE_DEV:
            schema = self.dev_schema
        results = await conn.fetch(
            _GRANULARITY_TEST.format(
                columns=",".join(columns), schema=schema, table=table
            )
        )
        await conn.close()
        return len(results) == 0, f'granularity_({",".join(columns)})'

    async def run_not_null_test(self, columns: List[str]):
        conn = await asyncpg.connect(dsn=self.dsn)
        schema, table = self.task_id.lower().split(".")
        if self.stage == _constants._STAGE_DEV:
            schema = self.dev_schema
        results = await conn.fetch(
            _NOT_NULL_TEST.format(
                columns="AND ".join([f"{column} IS NULL " for column in columns]),
                schema=schema,
                table=table,
            )
        )
        await conn.close()
        return len(results) == 0, f'not-null_({",".join(columns)})'

    async def run_relationship_test(self, relationship: str, task_ids: Set[str]):
        conn = await asyncpg.connect(dsn=self.dsn)
        schema, table = self.task_id.lower().split(".")
        if self.stage == _constants._STAGE_DEV:
            schema = self.dev_schema
        column, raw_relationship_column = relationship.replace(" ", "").split("=")
        r_schema, r_table, r_column = raw_relationship_column.split(".")
        if f"{r_schema}.{r_table}".lower() in {task_id.lower() for task_id in task_ids}:
            r_schema = self.dev_schema
        results = await conn.fetch(
            _RELATIONSHIP_TEST.format(
                schema=schema,
                table=table,
                column=column,
                r_schema=r_schema,
                r_table=r_table,
                r_column=r_column,
            )
        )
        await conn.close()
        return len(results) == 0, f"relationship_({relationship.replace(' ', '')})"

    def _replace_for_dev(self, query: str, task_ids: Set[str]) -> str:
        repl = rf"\g<before>{self.dev_schema}\g<after>"

        def repl_fn(match: re.Match):
            schema_table = match.groups()[0].lower()
            if schema_table in task_ids:
                # Should do the replacement
                _, table = schema_table.split(".")
                return match.group(0).replace(
                    match.groups()[0], ".".join((self.dev_schema, table))
                )
            return match.group(0)

        updated_query = re.sub(_CREATE_TABLE_REGEXP, repl, query)
        updated_query = re.sub(_DROP_TABLE_REGEXP, repl, updated_query)
        updated_query = re.sub(_DELETE_REGEXP, repl, updated_query)
        updated_query = re.sub(_FROM_JOIN_REGEXP, repl_fn, updated_query)
        updated_query = re.sub(_INSERT_REGEXP, repl, updated_query)
        updated_query = re.sub(_UPDATE_REGEXP, repl, updated_query)
        return updated_query

    def _create_cache_key(self, ddl_script: str, insert_script: str):
        return f'{hashlib.sha256(ddl_script.encode("utf-8")).hexdigest()}_{hashlib.sha256(insert_script.encode("utf-8")).hexdigest()}'

    def _set_cache(self, ddl_script: str, insert_script: str):
        with open(self.cache_filename, "w") as cache_file:
            cache_file.write(f"{self._create_cache_key(ddl_script, insert_script)}")
            cache_file.write(",")
            cache_file.write(f"{time.time()}")

    def _check_is_cached(self, ddl_script: str, insert_script: str):
        if not os.path.exists(self.cache_filename):
            return False
        with open(self.cache_filename, "r") as cache_file:
            cache_file_contents = cache_file.read()
        try:
            cache_key, cache_set_time = cache_file_contents.split(",")
            cache_set_time = float(cache_set_time)
        except:
            os.remove(self.cache_filename)
            return False
        if time.time() - cache_set_time > self.cache_duration:
            os.remove(self.cache_filename)
            return False
        if not self._create_cache_key(ddl_script, insert_script) == cache_key:
            os.remove(self.cache_filename)
            return False
        return True

    async def execute(self, task_ids: Set[str]):
        self.status = SQLTaskStatus.RUNNING
        try:
            ddl_script = self.get_ddl()
            insert_script = self.get_insert()
            if self.stage == _constants._STAGE_DEV:
                ddl_script = self._replace_for_dev(ddl_script, task_ids)
                insert_script = self._replace_for_dev(insert_script, task_ids)
                if not self.no_cache and self._check_is_cached(
                    ddl_script, insert_script
                ):
                    w_print(f"Task {self.task_id.lower()} cached.")
                    self.status = SQLTaskStatus.SUCCESS
                    return

            conn = await asyncpg.connect(dsn=self.dsn)
            async with conn.transaction():
                await conn.execute(ddl_script)
                await conn.execute(insert_script)
            await conn.close()

            test_futures = []
            granularity_columns_match = _GRANULARITY_TEST_REGEXP.search(insert_script)
            if granularity_columns_match is not None:
                columns = [
                    column.strip()
                    for column in granularity_columns_match.group(1).split(",")
                ]
                test_futures.append(
                    asyncio.create_task(
                        self.run_granularity_test(columns),
                        name=f"{self.task_id}-granularity-test",
                    )
                )

            not_null_columns_match = _NOT_NULL_TEST_REGEXP.search(insert_script)
            if not_null_columns_match is not None:
                columns = [
                    column.strip()
                    for column in not_null_columns_match.group(1).split(",")
                ]
                test_futures.append(
                    asyncio.create_task(
                        self.run_not_null_test(columns),
                        name=f"{self.task_id}-not-null-test",
                    )
                )

            relationship_match = _RELATIONSHIP_TEST_REGEXP.findall(insert_script)
            for relationship in relationship_match:
                test_futures.append(
                    asyncio.create_task(
                        self.run_relationship_test(relationship, task_ids),
                        name=f"{self.task_id}-relationship-{relationship}-test",
                    )
                )

            if len(test_futures) > 0:
                w_print(
                    f"Running {len(test_futures)} tests for {self.task_id.lower()}."
                )
            for test in asyncio.as_completed(test_futures):
                result, test_name = await test
                if not result:
                    self.failed_tests.append(test_name)

            if len(self.failed_tests) > 0:
                w_print(
                    f"Task {self.task_id.lower()} failed {len(self.failed_tests)} tests."
                )
                self.status = SQLTaskStatus.TEST_FAILED
                return

        except Exception as e:
            w_print(f"Task {self.task_id.lower()} failed:")
            print(e)
            self.status = SQLTaskStatus.FAILED
            return
        if self.stage == _constants._STAGE_DEV:
            self._set_cache(ddl_script, insert_script)
        w_print(f"Task {self.task_id.lower()} complete.")
        self.status = SQLTaskStatus.SUCCESS
