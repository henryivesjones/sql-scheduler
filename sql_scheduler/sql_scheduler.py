import asyncio
import os
from datetime import datetime, timedelta
from typing import List, Literal, Optional, Tuple

from . import _constants
from .exceptions import *
from .orchestrator import SQLOrchestrator


def sql_scheduler(
    ddl_directory: Optional[str] = None,
    insert_directory: Optional[str] = None,
    dsn: Optional[str] = None,
    incremental_interval: Optional[Tuple[datetime, datetime]] = None,
    targets: Optional[List[str]] = None,
    dependencies: bool = False,
    stage: Optional[Literal["prod", "dev"]] = None,
    dev_schema: Optional[str] = None,
    cache_duration: Optional[int] = None,
    refill: bool = False,
    no_cache: bool = False,
    concurrency: Optional[int] = None,
    simple_output: Optional[bool] = None,
    verbose: bool = False,
) -> int:
    """
    Programmatic entrypoint to sql_scheduler. All inputs passed into this function will override any environment variables.

    Returns the # of failed tasks or tasks which didn't run because of upstream failures.
    """
    env_ddl_directory = os.environ.get(_constants._DDL_DIR_ENVVAR)
    env_insert_directory = os.environ.get(_constants._INSERT_DIR_ENVVAR)
    env_dsn = os.environ.get(_constants._DSN_ENVVAR)
    env_stage = os.environ.get(_constants._STAGE_ENVVAR, _constants._STAGE_PROD)
    env_cache_duration = os.environ.get(
        _constants._CACHE_DURATION_ENVVAR, _constants._BASE_CACHE_DURATION
    )
    env_dev_schema = os.environ.get(_constants._DEV_SCHEMA_ENVVAR)
    env_concurrency = os.environ.get(
        _constants._CONCURRENCY_ENVVAR, _constants._BASE_CONCURRENCY
    )
    env_simple_output = os.environ.get(_constants._SIMPLE_OUTPUT_ENVVAR, False)
    env_incremental_interval_duration = os.environ.get(
        _constants._INCREMENTAL_INTERVAL_ENVVAR, _constants._BASE_INCREMENTAL_DURATION
    )

    if ddl_directory is None:
        if env_ddl_directory is None:
            raise SQLSchedulerInvalidDDLDirectory()
        ddl_directory = env_ddl_directory

    if insert_directory is None:
        if env_insert_directory is None:
            raise SQLSchedulerInvalidInsertDirectory()
        insert_directory = env_insert_directory

    if dsn is None:
        if env_dsn is None:
            raise SQLSchedulerNoDSN()
        dsn = env_dsn

    if stage is None:
        if not (
            env_stage == _constants._STAGE_DEV or env_stage == _constants._STAGE_PROD
        ):
            raise SQLSchedulerInvalidStage(env_stage)
        stage = env_stage

    if cache_duration is None:
        try:
            cache_duration = int(env_cache_duration)
        except ValueError:
            raise SQLSchedulerInvalidCacheDuration(env_cache_duration)

    if dev_schema is None and env_dev_schema is not None:
        dev_schema = env_dev_schema

    if concurrency is None:
        try:
            concurrency = int(env_concurrency)
        except ValueError:
            raise SQLSchedulerInvalidConcurrency(env_cache_duration)

    if simple_output is None:
        simple_output = bool(env_simple_output)

    if stage == _constants._STAGE_DEV and dev_schema is None:
        raise SQLSchedulerNoDevSchema()

    orchestrator = SQLOrchestrator(
        ddl_directory=ddl_directory, insert_directory=insert_directory
    )

    if incremental_interval is None:
        now = datetime.today()
        try:
            env_incremental_interval_duration = int(env_incremental_interval_duration)
        except ValueError:
            raise SQLSchedulerInvalidIntervalDuration()
        incremental_interval_start = datetime(now.year, now.month, now.day) - timedelta(
            days=env_incremental_interval_duration
        )
        incremental_interval_end = datetime(now.year, now.month, now.day) + timedelta(
            days=1, milliseconds=-0.001
        )
        incremental_interval = (incremental_interval_start, incremental_interval_end)

    return asyncio.run(
        orchestrator.execute(
            dsn=dsn,
            incremental_interval=incremental_interval,
            targets=targets,
            dependencies=dependencies,
            stage=stage,
            dev_schema=dev_schema,
            cache_duration=cache_duration,
            refill=refill,
            no_cache=no_cache,
            concurrency=concurrency,
            simple_output=simple_output,
            verbose=verbose,
        )
    )
