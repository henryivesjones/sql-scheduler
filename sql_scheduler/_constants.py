import os

_EVENT_LOOP_SLEEP = 0.25

_CACHE_DIR = os.path.join(os.path.expanduser("~"), ".sql-scheduler/cache")
_BASE_CACHE_DURATION = 6 * 60 * 60  # 6 Hours
_BASE_INCREMENTAL_DURATION = 14

_TASK_FILE_ENDING = ".sql"

_ENVVAR_PREFIX = "SQL_SCHEDULER"
_DDL_DIR_ENVVAR = f"{_ENVVAR_PREFIX}_DDL_DIRECTORY"
_INSERT_DIR_ENVVAR = f"{_ENVVAR_PREFIX}_INSERT_DIRECTORY"
_STAGE_ENVVAR = f"{_ENVVAR_PREFIX}_STAGE"
_DEV_SCHEMA_ENVVAR = f"{_ENVVAR_PREFIX}_DEV_SCHEMA"
_DSN_ENVVAR = f"{_ENVVAR_PREFIX}_DSN"
_SIMPLE_OUTPUT_ENVVAR = f"{_ENVVAR_PREFIX}_SIMPLE_OUTPUT"
_CACHE_DURATION_ENVVAR = f"{_ENVVAR_PREFIX}_CACHE_DURATION"
_INCREMENTAL_INTERVAL_ENVVAR = f"{_ENVVAR_PREFIX}_INCREMENTAL_INTERVAL"

_STAGE_PROD = "prod"
_STAGE_DEV = "dev"


_DESCRIPTION = """
A task runner/scheduler for running SQL tasks against a Postgres/Redshift Database.
Automatically infers dependencies between scripts and runs the tasks in the correct order.
""".strip()

_EPILOG = f"""
Several environment variables are required to be set in order for this to function:
{_DDL_DIR_ENVVAR}
{_INSERT_DIR_ENVVAR}
{_DSN_ENVVAR}
""".strip()
