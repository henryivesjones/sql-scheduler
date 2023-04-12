import asyncio
import os
import shutil
import sys
from datetime import datetime, timedelta
from typing import Any, Literal, Optional, Tuple, Union

import click
from dateutil import parser as dt_parser

from sql_scheduler import __version__

from ._constants import (
    _BASE_CACHE_DURATION,
    _BASE_CONCURRENCY,
    _BASE_INCREMENTAL_DURATION,
    _CACHE_DIR,
    _CACHE_DURATION_ENVVAR,
    _CONCURRENCY_ENVVAR,
    _DDL_DIR_ENVVAR,
    _DEV_SCHEMA_ENVVAR,
    _DSN_ENVVAR,
    _INCREMENTAL_INTERVAL_ENVVAR,
    _INSERT_DIR_ENVVAR,
    _SIMPLE_OUTPUT_ENVVAR,
    _STAGE_DEV,
    _STAGE_PROD,
)
from .orchestrator import (
    SQLOrchestrator,
    SQLSchedulerCycleFound,
    SQLSchedulerInvalidDDLDirectory,
    SQLSchedulerInvalidInsertDirectory,
    SQLSchedulerNoDevSchema,
)


class DateTimeParamType(click.ParamType):
    name: str = "DATETIME"

    def convert(
        self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ):
        if value is None:
            return value
        if isinstance(value, datetime):
            return value
        try:
            return dt_parser.parse(value)
        except dt_parser.ParserError:
            self.fail(f"{value} is not a valid date", param, ctx)


DATETIME_PARAM_TYPE = DateTimeParamType()

incremental_interval_duration = int(
    os.environ.get(
        _INCREMENTAL_INTERVAL_ENVVAR,
        _BASE_INCREMENTAL_DURATION,
    )
)
now = datetime.today()
incremental_interval_start = datetime(now.year, now.month, now.day) - timedelta(
    days=incremental_interval_duration
)
incremental_interval_end = datetime(now.year, now.month, now.day) + timedelta(
    days=1, milliseconds=-0.001
)


@click.command(
    "execute",
    context_settings=dict(max_content_width=240, help_option_names=["-h", "--help"]),
)
@click.option(
    "--prod/--dev",
    "is_prod",
    default=None,
    help="Run SQL tasks in the dev or prod schema. [env var: SQL_SCHEDULER_STAGE]",
    show_default=True,
)
@click.option(
    "--dev-schema",
    envvar=_DEV_SCHEMA_ENVVAR,
    help="The dev schema to replace schemas with in SQL statements.",
    show_envvar=True,
)
@click.option(
    "-t",
    "--target",
    multiple=True,
    help="Specific tasks to be run instead of a complete run.",
)
@click.option(
    "-e", "--exclusion", multiple=True, help="Exclude specific tasks from being run."
)
@click.option(
    "--dependencies",
    is_flag=True,
    default=False,
    help="Flag to run the upstream dependencies of the given targets.",
)
@click.option(
    "--no-cache",
    is_flag=True,
    default=False,
    help="Flag to not use cache for development run. Forces a complete run of dependencies.",
)
@click.option(
    "--refill",
    is_flag=True,
    default=False,
    help="Force drop and recreate of all incremental tables.",
)
@click.option(
    "--start",
    type=DATETIME_PARAM_TYPE,
    default=incremental_interval_start,
    show_default=True,
    help="Start datetime for incremental table updates.",
)
@click.option(
    "--end",
    type=DATETIME_PARAM_TYPE,
    default=incremental_interval_end,
    show_default=True,
    help="End datetime for incremental table updates.",
)
@click.option(
    "--check",
    is_flag=True,
    default=False,
    help="Do a check for circular dependency without running any scripts.",
)
@click.option(
    "--clear-cache", is_flag=True, default=False, help="Clears development cache."
)
@click.option(
    "--ddl-dir",
    default=None,
    envvar=_DDL_DIR_ENVVAR,
    help="The DDL directory.",
    show_envvar=True,
)
@click.option(
    "--insert-dir",
    default=None,
    envvar=_INSERT_DIR_ENVVAR,
    show_envvar=True,
    help="The Insert directory.",
)
@click.option(
    "--dsn",
    default=None,
    envvar=_DSN_ENVVAR,
    help="DB Data Source Name.",
    show_envvar=True,
)
@click.option(
    "--cache-duration",
    default=_BASE_CACHE_DURATION,
    envvar=_CACHE_DURATION_ENVVAR,
    show_envvar=True,
    show_default=True,
    help="Cache persistence in seconds.",
    type=int,
)
@click.option(
    "--concurrency",
    default=_BASE_CONCURRENCY,
    envvar=_CONCURRENCY_ENVVAR,
    show_default=True,
    show_envvar=True,
    help="Max # of concurrent tasks to be running at the same time.",
    type=int,
)
@click.option(
    "--simple-output",
    default=False,
    is_flag=True,
    envvar=_SIMPLE_OUTPUT_ENVVAR,
    show_envvar=True,
    help="Don't log the in-place status message.",
)
@click.option(
    "--version", default=False, is_flag=True, help="Return the version and exit."
)
@click.option(
    "--verbose",
    default=False,
    is_flag=True,
    help="Log all queries made by sql-scheduler",
)
def entrypoint(
    is_prod: Optional[bool],
    dev_schema: str,
    target: Tuple[str],
    exclusion: Tuple[str],
    dependencies: bool,
    no_cache: bool,
    refill: bool,
    start: datetime,
    end: datetime,
    check: bool,
    clear_cache: bool,
    ddl_dir: Optional[str],
    insert_dir: Optional[str],
    dsn: Optional[str],
    cache_duration: int,
    concurrency: int,
    simple_output: bool,
    version: bool,
    verbose: bool,
):
    """
    A task runner/scheduler for running SQL tasks against a Postgres/Redshift Database.
    Automatically infers dependencies between scripts and runs the tasks in the correct order.
    """
    if version:
        click.echo(f"sql-scheduler v{__version__}")
        return
    os.makedirs(_CACHE_DIR, exist_ok=True)
    stage: Union[Literal["dev"], Literal["prod"]] = _STAGE_PROD
    env_stage = os.environ.get("SQL_SCHEDULER_STAGE")
    if is_prod is None:
        if env_stage is not None:
            env_stage = env_stage.lower()
            if env_stage in (_STAGE_PROD, _STAGE_DEV):
                stage = env_stage
    else:
        stage = _STAGE_PROD if is_prod else _STAGE_DEV

    if clear_cache:
        shutil.rmtree(_CACHE_DIR)
        return

    if ddl_dir is None:
        raise click.ClickException(f"DDL directory must be provided.")
    if insert_dir is None:
        raise click.ClickException(f"Insert directory must be provided.")
    if dsn is None:
        raise click.ClickException("DSN must be provided.")

    try:
        orchestrator = SQLOrchestrator(
            ddl_directory=ddl_dir, insert_directory=insert_dir
        )
    except SQLSchedulerInvalidDDLDirectory:
        raise click.ClickException(
            f"DDL directory '{ddl_dir}' doesn't exist or isn't a dir."
        )
    except SQLSchedulerInvalidInsertDirectory:
        raise click.ClickException(
            f"Insert directory '{insert_dir}' doesn't exist or isn't a dir."
        )

    if check:
        circular = orchestrator._circular_check()
        if circular:
            raise click.ClickException("Cycle found between tasks.")
        return

    targets = None if len(target) == 0 else list(target)
    exclusions = None if len(exclusion) == 0 else list(exclusion)
    try:
        exit_code = asyncio.run(
            orchestrator.execute(
                dsn=dsn,
                incremental_interval=(start, end),
                targets=targets,
                exclusions=exclusions,
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

    except SQLSchedulerNoDevSchema:
        raise click.ClickException(
            "A dev schema must be set when running in the dev stage"
        )
    except SQLSchedulerCycleFound:
        raise click.ClickException("A cycle was found.")

    if exit_code > 0:
        sys.exit(exit_code)


if __name__ == "__main__":
    entrypoint()
