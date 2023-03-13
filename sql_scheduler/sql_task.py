import asyncio
import hashlib
import os
import re
import time
from datetime import datetime
from enum import Enum
from typing import List, Literal, Set, Tuple, Union

import asyncpg

from . import _constants
from ._helpers import w_print


class SQLTaskDDLFileNotExists(Exception):
    pass


class SQLTaskInsertFileNotExists(Exception):
    pass


_MULTILINE_COMMENT_REGEXP = re.compile(r"""\/\*[\s\S]*?\*\/""", flags=re.IGNORECASE)

_COMMENT_REGEXP = re.compile(
    r"""(?<!')--[^\r\n]*?$""", flags=re.IGNORECASE | re.MULTILINE
)

_FROM_JOIN_REGEXP = re.compile(
    r"""(?<!delete\s)(?:from|join)\s+(?P<after>"?[\w\d]*?"?\."?[\w\d]*"?)\s*""",
    flags=re.IGNORECASE,
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
    r'(?P<before>delete\s+from\s*"?)([\w\d]*)(?P<after>"?\."?[\w\d]*"?)',
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

_UPSTREAM_COUNT_TEST_REGEXP = re.compile(
    r"""upstream_count:\s*?"?([\w_\\\/]+?)"?\."?([\w_\\\/]+?)"?\s+?(\d+?)[\s\*]""",
    flags=re.IGNORECASE,
)

_COUNT_TEST = """
SELECT COUNT(1) as count
FROM "{schema}"."{table}"
;
""".strip()

_UPSTREAM_GRANULARITY_TEST_REGEXP = re.compile(
    r"""upstream_granularity:\s*?"?([\w_\\\/]+?)"?\."?([\w_\\\/]+?)"?\s+([\w_\\\/ ,]+)\*?""",
    flags=re.IGNORECASE,
)

_INCREMENTAL_REGEXP = re.compile(r"--sql-scheduler-incremental", flags=re.IGNORECASE)


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
    verbose: bool
    failed_tests: List[str]
    cache_duration: int
    cache_filename: str
    no_cache: bool
    start_timestamp: float = 0
    script_duration: Union[float, None] = None
    test_start_timestamp: Union[float, None] = None
    test_duration: Union[float, None] = None
    upstream_test_start_timestamp: Union[float, None] = None
    upstream_test_duration: Union[float, None] = None

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
        verbose: bool,
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
        self.verbose = verbose

        self.dependencies = self._parse_dependencies()
        self.incremental = self._is_incremental()
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

    def _clean_sql_script(self, query: str) -> str:
        """removes comments from sql scripts"""
        cleaned_query = _COMMENT_REGEXP.sub("", query)
        cleaned_query = _MULTILINE_COMMENT_REGEXP.sub("", cleaned_query)
        return cleaned_query

    def _parse_dependencies(self) -> Set[str]:
        return set(
            [
                table.lower().replace('"', "")
                for table in _FROM_JOIN_REGEXP.findall(
                    self._clean_sql_script(self.get_insert())
                )
            ]
        )

    def _is_incremental(self):
        match = _INCREMENTAL_REGEXP.match(self.get_insert())
        return match is not None

    def remove_second_class_dependencies(
        self, first_class_dependencies: Set[str]
    ) -> None:
        self.dependencies.intersection_update(first_class_dependencies)

    async def run_granularity_test(self, columns: List[str]):
        conn = await asyncpg.connect(dsn=self.dsn)
        schema, table = self.task_id.lower().split(".")
        if self.stage == _constants._STAGE_DEV:
            schema = self.dev_schema
        query = _GRANULARITY_TEST.format(
            columns=",".join(columns), schema=schema, table=table
        )
        if self.verbose:
            w_print(query)
        results = await conn.fetch(query)
        await conn.close()
        return len(results) == 0, f'granularity_({",".join(columns)})'

    async def run_not_null_test(self, columns: List[str]):
        conn = await asyncpg.connect(dsn=self.dsn)
        schema, table = self.task_id.lower().split(".")
        if self.stage == _constants._STAGE_DEV:
            schema = self.dev_schema
        query = _NOT_NULL_TEST.format(
            columns="AND ".join([f"{column} IS NULL " for column in columns]),
            schema=schema,
            table=table,
        )
        if self.verbose:
            w_print(query)
        results = await conn.fetch(query)
        await conn.close()
        return len(results) == 0, f'not-null_({",".join(columns)})'

    async def run_relationship_test(self, relationship: str, task_ids: Set[str]):
        conn = await asyncpg.connect(dsn=self.dsn)
        schema, table = self.task_id.lower().split(".")
        if self.stage == _constants._STAGE_DEV:
            schema = self.dev_schema
        column, raw_relationship_column = relationship.replace(" ", "").split("=")
        r_schema, r_table, r_column = raw_relationship_column.split(".")
        if (
            f"{r_schema}.{r_table}".lower() in {task_id.lower() for task_id in task_ids}
            and self.stage == _constants._STAGE_DEV
        ):
            r_schema = self.dev_schema
        query = _RELATIONSHIP_TEST.format(
            schema=schema,
            table=table,
            column=column,
            r_schema=r_schema,
            r_table=r_table,
            r_column=r_column,
        )
        if self.verbose:
            w_print(query)
        results = await conn.fetch(query)
        await conn.close()
        return len(results) == 0, f"relationship_({relationship.replace(' ', '')})"

    async def run_count_test(self, schema: str, table: str, count: int):
        conn = await asyncpg.connect(dsn=self.dsn)
        test_name = f"count_({schema}.{table}_{count})"
        query = _COUNT_TEST.format(schema=schema, table=table)
        if self.verbose:
            w_print(query)
        results = await conn.fetch(query)
        if len(results) != 1:
            return False, test_name
        return results[0]["count"] > count, test_name

    async def run_upstream_granularity_test(
        self, schema: str, table: str, columns: List[str]
    ):
        conn = await asyncpg.connect(dsn=self.dsn)
        query = _GRANULARITY_TEST.format(
            columns=",".join(columns), schema=schema, table=table
        )
        if self.verbose:
            w_print(query)
        results = await conn.fetch(query)

        return (
            len(results) == 0,
            f'upstream_granularity({schema}.{table} | {",".join(columns)})',
        )

    def _replace_for_dev(self, query: str, task_ids: Set[str]) -> str:
        repl = rf"\g<before>{self.dev_schema}\g<after>"

        def repl_fn(match: re.Match):
            schema_table = match.groups()[0].lower().replace('"', "")
            if schema_table in task_ids:
                # Should do the replacement
                _, table = schema_table.split(".")
                return match.group(0).replace(
                    match.groups()[0], ".".join((self.dev_schema, table))
                )
            return match.group(0)

        updated_query = re.sub(_CREATE_TABLE_REGEXP, repl, query)
        updated_query = re.sub(_DROP_TABLE_REGEXP, repl, updated_query)
        updated_query = re.sub(_FROM_JOIN_REGEXP, repl_fn, updated_query)
        updated_query = re.sub(_DELETE_REGEXP, repl, updated_query)
        updated_query = re.sub(_INSERT_REGEXP, repl, updated_query)
        updated_query = re.sub(_UPDATE_REGEXP, repl, updated_query)
        return updated_query

    def _create_cache_key(self, ddl_script: str, insert_script: str):
        return f'{hashlib.sha256(ddl_script.encode("utf-8")).hexdigest()}_{hashlib.sha256(insert_script.encode("utf-8")).hexdigest()}'

    def _set_cache(self, ddl_script: str, insert_script: str):
        with open(self.cache_filename, "w") as cache_file:
            cache_file.write(self._create_cache_key(ddl_script, insert_script))
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

    def _get_analyze(self):
        schema, table = self.task_id.lower().split(".")
        if self.stage == _constants._STAGE_DEV:
            schema = self.dev_schema
        return f"ANALYZE {schema}.{table};"

    async def execute(
        self,
        task_ids: Set[str],
        incremental_interval: Tuple[datetime, datetime],
        refill: bool = True,
    ):
        self.status = SQLTaskStatus.RUNNING
        self.start_timestamp = time.time()
        try:
            ddl_script = self.get_ddl()
            insert_script = self.get_insert()
            if self.incremental:
                insert_script = insert_script.replace(
                    "$1",
                    "'"
                    + incremental_interval[0].strftime("%Y-%m-%d %H:%M:%S")
                    + "'::timestamp",
                ).replace(
                    "$2",
                    "'"
                    + incremental_interval[1].strftime("%Y-%m-%d %H:%M:%S")
                    + "'::timestamp",
                )
            if self.stage == _constants._STAGE_DEV:
                ddl_script = self._replace_for_dev(ddl_script, task_ids)
                insert_script = self._replace_for_dev(insert_script, task_ids)
                if not self.no_cache and self._check_is_cached(
                    ddl_script, insert_script
                ):
                    w_print(f"Task {self.task_id.lower()} cached.")
                    self.status = SQLTaskStatus.SUCCESS
                    return
            self.upstream_test_start_timestamp = time.time()
            upstream_test_futures = []

            upstream_count_matches = _UPSTREAM_COUNT_TEST_REGEXP.finditer(insert_script)
            for upstream_count_match in upstream_count_matches:
                schema = upstream_count_match.group(1)
                table = upstream_count_match.group(2)
                count = upstream_count_match.group(3)
                try:
                    count = int(count)
                except TypeError:
                    self.failed_tests.append(
                        f"upstream_count-{schema}-{table}-count-parse_error"
                    )
                    continue
                except ValueError:
                    self.failed_tests.append(
                        f"upstream_count-{schema}-{table}-count-parse_error"
                    )
                    continue
                upstream_test_futures.append(
                    asyncio.create_task(
                        self.run_count_test(schema, table, count),
                        name=f"{self.task_id}-upstream-count-test-{schema}-{table}",
                    )
                )

            upstream_granularity_matches = _UPSTREAM_GRANULARITY_TEST_REGEXP.finditer(
                insert_script
            )
            for upstream_granularity_match in upstream_granularity_matches:
                schema = upstream_granularity_match.group(1)
                table = upstream_granularity_match.group(2)
                columns = [
                    col.strip()
                    for col in upstream_granularity_match.group(3).split(",")
                ]
                upstream_test_futures.append(
                    asyncio.create_task(
                        self.run_upstream_granularity_test(schema, table, columns),
                        name=f"{self.task_id}-upstream-granularity-test-{schema}-{table}",
                    )
                )
            if len(upstream_test_futures) > 0:
                w_print(
                    f"Running {len(upstream_test_futures)} upstream tests for {self.task_id}."
                )
            for test in asyncio.as_completed(upstream_test_futures):
                result, test_name = await test
                if not result:
                    self.failed_tests.append(test_name)
            self.upstream_test_duration = (
                time.time() - self.upstream_test_start_timestamp
            )
            if len(self.failed_tests) > 0:
                w_print(
                    f"Task {self.task_id.lower()} failed {len(self.failed_tests)} upstream tests."
                )
                self.status = SQLTaskStatus.TEST_FAILED
                return

            conn = await asyncpg.connect(dsn=self.dsn)

            need_to_create_table = False
            if self.incremental and refill is False:
                schema, table = self.task_id.lower().split(".")
                if self.stage == _constants._STAGE_DEV:
                    schema = self.dev_schema
                if self.verbose:
                    w_print(
                        f"select 1 from INFORMATION_SCHEMA.tables where table_schema = '{schema}' and table_name = '{table}' LIMIT 1;"
                    )
                result = await conn.fetch(
                    "select 1 from INFORMATION_SCHEMA.tables where table_schema = $1 and table_name = $2 LIMIT 1;",
                    schema,
                    table,
                )
                if len(result) == 0:
                    need_to_create_table = True
                # check if table exists
            async with conn.transaction():
                if not self.incremental or refill is True or need_to_create_table:
                    if self.verbose:
                        w_print(ddl_script)
                    await conn.execute(ddl_script)
                if self.verbose:
                    w_print(insert_script)
                await conn.execute(insert_script)
                if self.verbose:
                    w_print(self._get_analyze())
                await conn.execute(self._get_analyze())
            await conn.close()
            self.script_duration = time.time() - self.start_timestamp

            self.test_start_timestamp = time.time()
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
            self.test_duration = time.time() - self.test_start_timestamp

            if len(self.failed_tests) > 0:
                w_print(
                    f"Task {self.task_id.lower()} failed {len(self.failed_tests)} tests."
                )
                self.status = SQLTaskStatus.TEST_FAILED
                return

        except Exception as e:
            w_print(f"Task {self.task_id.lower()} failed:")
            print(e)
            if self.script_duration is None:
                self.script_duration = time.time() - self.start_timestamp
            self.status = SQLTaskStatus.FAILED
            return
        if self.stage == _constants._STAGE_DEV and not self.no_cache:
            self._set_cache(ddl_script, insert_script)
        w_print(f"Task {self.task_id.lower()} complete.")
        self.status = SQLTaskStatus.SUCCESS
