import argparse
import asyncio
import os
import sys
import time
from datetime import timedelta
from typing import List, Literal, Optional, Set

from . import _constants
from ._helpers import _SIMPLE_OUTPUT, w_print
from .sql_task import SQLTask, SQLTaskStatus


def _parse_arguments():
    parser = argparse.ArgumentParser(
        epilog=_constants._EPILOG, description=_constants._DESCRIPTION
    )
    parser.add_argument(
        "--dev",
        action="store_const",
        const="dev",
        dest="stage",
        help="Run SQL tasks in the dev schema. Must also include a dev schema. Overrides the SQL_SCHEDULER_STAGE Envvar.",
    )
    parser.add_argument(
        "--prod",
        action="store_const",
        const="prod",
        dest="stage",
        help="Run SQL tasks in the prod schema. Overrides the SQL_SCHEDULER_STAGE Envvar.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        dest="check",
        default=False,
        help="Do a check for circular dependency without running any scripts.",
    )
    parser.add_argument(
        "--dev-schema",
        dest="dev_schema",
        help="The dev schema to replace schemas with in SQL statements.",
    )
    parser.add_argument(
        "--target",
        "-t",
        action="append",
        dest="targets",
        help="Specific tasks to be run instead of a complete run.",
    )
    parser.add_argument(
        "--dependencies",
        action="store_true",
        default=False,
        help="Flag to run the upstream dependencies of the given targets",
    )
    args = parser.parse_args()
    stage = args.stage
    if args.stage is None and args.dev_schema is not None:
        stage = _constants._STAGE_DEV
    return stage, args.dev_schema, args.targets, args.dependencies, args.check


def _get_task_parents(task: SQLTask, tasks: List[SQLTask]) -> Set[SQLTask]:
    if len(task.dependencies) == 0:
        return {task}
    parents = {task}
    for dependency in task.dependencies:
        parent_task = [
            _task for _task in tasks if _task.task_id.lower() == dependency.lower()
        ]
        if len(parent_task) != 1:
            print(
                f"FATAL ERROR: task {task.task_id} has a non-existent dependency {dependency}."
            )
            sys.exit(1)

        parents.update(_get_task_parents(parent_task[0], tasks))
    return parents


def _recursive_circular_check(
    task: SQLTask, tasks: List[SQLTask], history: List[str] = []
) -> int:
    """Return 1 if a circular dependency was found"""
    if task.task_id.lower() in history:
        print(f'Circular dependency found between tasks {",".join(history)}.')
        return 1
    results = 0
    for dependency in task.dependencies:
        parent_task = [
            _task for _task in tasks if _task.task_id.lower() == dependency.lower()
        ]
        if len(parent_task) != 1:
            print(
                f"FATAL ERROR: task {task.task_id} has a non-existent dependency {dependency}."
            )
            sys.exit(1)
        results += _recursive_circular_check(
            parent_task[0], tasks, [*history, task.task_id.lower()]
        )

    return results


def _circular_check(tasks: List[SQLTask]) -> bool:
    """Return True if Cycle found."""
    result = 0
    for task in tasks:
        result += _recursive_circular_check(task, tasks, history=[])
    return result > 0


def _parse_tasks(
    ddl_directory: str,
    insert_directory: str,
    stage: Literal["prod", "dev"],
    dev_schema: str,
    dsn: str,
) -> List[SQLTask]:
    return [
        SQLTask(
            ddl_directory,
            insert_directory,
            filename[: -1 * len(_constants._TASK_FILE_ENDING)],
            stage=stage,
            dev_schema=dev_schema,
            dsn=dsn,
        )
        for filename in os.listdir(insert_directory)
        if filename[-1 * len(_constants._TASK_FILE_ENDING) :]
        == _constants._TASK_FILE_ENDING
    ]


def _should_start_task(task: SQLTask, tasks: List[SQLTask]) -> bool:
    task_statuses = {task.task_id.lower(): task.status for task in tasks}
    if task.status != SQLTaskStatus.WAITING:
        return False
    for task_id in task.dependencies:
        task_status = task_statuses.get(task_id)
        if task_status in [
            SQLTaskStatus.UPSTREAM_FAILED,
            SQLTaskStatus.FAILED,
            SQLTaskStatus.TEST_FAILED,
        ]:
            w_print(
                f"Marking task {task.task_id.lower()} as UPSTREAM_FAILED due to {task_id} ({task_statuses[task_id].name})"
            )
            task.status = SQLTaskStatus.UPSTREAM_FAILED
            return False
        if task_status in [
            SQLTaskStatus.WAITING,
            SQLTaskStatus.RUNNING,
        ]:
            return False
    return True


async def execute(
    stage: Optional[Literal["dev", "prod"]] = None,
    dev_schema: Optional[str] = None,
    targets: Optional[List[str]] = None,
    dependencies: bool = False,
    check: bool = False,
) -> List[SQLTask]:
    start_time = time.time()
    # PARSE STAGE (dev, prod)
    if stage is None:
        stage = os.environ.get(_constants._STAGE_ENVVAR, _constants._STAGE_PROD)  # type: ignore
        if stage not in (_constants._STAGE_PROD, _constants._STAGE_DEV):
            stage = _constants._STAGE_PROD

    # PARSE DEV SCHEMA
    if dev_schema is None:
        dev_schema = os.environ.get(_constants._DEV_SCHEMA_ENVVAR, "")
    if stage == _constants._STAGE_DEV and dev_schema == "":
        print(
            f"No dev schema provided when stage is set to dev. {_constants._DEV_SCHEMA_ENVVAR} must be set."
        )
        sys.exit(1)

    # PARSE DDL DIR
    ddl_directory = os.environ.get(_constants._DDL_DIR_ENVVAR)
    if ddl_directory is None:
        print(f"No DDL directory provided. {_constants._DDL_DIR_ENVVAR} must be set.")
        sys.exit(1)

    if not os.path.exists(ddl_directory):
        print(f"DDL Directory: {ddl_directory} is non-existent.")
        sys.exit(1)

    # PARSE INSERT DIR
    insert_directory = os.environ.get(_constants._INSERT_DIR_ENVVAR)
    if insert_directory is None:
        print(
            f"No INSERT directory provided. {_constants._INSERT_DIR_ENVVAR} must be set."
        )
        sys.exit(1)

    if not os.path.exists(insert_directory):
        print(f"INSERT Directory: {insert_directory} is non-existent.")
        sys.exit(1)

    # PARSE DSN
    dsn = os.environ.get(_constants._DSN_ENVVAR)
    if dsn is None:
        print(f"No dsn provided. {_constants._DSN_ENVVAR} must be set.")
        sys.exit(1)

    # PARSE TASKS
    tasks = _parse_tasks(
        ddl_directory,
        insert_directory,
        stage=stage,
        dev_schema=dev_schema or "",
        dsn=dsn,
    )
    for task in tasks:
        task.remove_second_class_dependencies({task.task_id.lower() for task in tasks})
    circular_dependencies = _circular_check(tasks)
    if circular_dependencies:
        print("Circular dependencies found... exiting.")
        sys.exit(1)
    if check:
        print("No circular dependencies found...")
        sys.exit()
    if targets is not None:
        if dependencies:
            print(f"Identifying upstream dependencies of {targets}...")
            subset_tasks: Set[SQLTask] = set()
            for target in targets:
                target_task = [_task for _task in tasks if _task.task_id == target]
                if len(target_task) != 1:
                    print(f"Target {target} is non-existent.")
                    sys.exit(1)
                subset_tasks.update(_get_task_parents(target_task[0], tasks))
            print(
                f"Found {len(subset_tasks) - len(targets)} tasks in upstream dependencies."
            )
            tasks = list(subset_tasks)
        else:
            tasks = [task for task in tasks if task.task_id in targets]
            if len(tasks) != len(targets):
                print(
                    f"Unknown Target: {set(targets) - {task.task_id for task in tasks}}"
                )
                sys.exit(1)
    task_ids = {task.task_id.lower() for task in tasks}

    # EXECUTION LOOP
    futures = []
    try:
        while True:
            for task in tasks:
                if _should_start_task(task, tasks):
                    w_print(f"Scheduling task {task.task_id.lower()} for execution.")
                    futures.append(
                        asyncio.create_task(
                            task.execute(task_ids),
                            name=task.task_id.lower(),
                        )
                    )
            if not {task.status for task in tasks}.isdisjoint(
                {SQLTaskStatus.WAITING, SQLTaskStatus.RUNNING}
            ):
                num_running_tasks = len(
                    [1 for task in tasks if task.status == SQLTaskStatus.RUNNING]
                )
                num_waiting_tasks = len(
                    [1 for task in tasks if task.status == SQLTaskStatus.WAITING]
                )
                num_completed_tasks = len(
                    [1 for task in tasks if task.status == SQLTaskStatus.SUCCESS]
                )
                num_failed_tasks = len(
                    [1 for task in tasks if task.status == SQLTaskStatus.FAILED]
                )
                num_upstream_failed_tasks = len(
                    [
                        1
                        for task in tasks
                        if task.status == SQLTaskStatus.UPSTREAM_FAILED
                    ]
                )
                num_failed_test_tasks = len(
                    [1 for task in tasks if task.status == SQLTaskStatus.TEST_FAILED]
                )
                if not _SIMPLE_OUTPUT:
                    w_print(
                        f"{num_running_tasks} running. "
                        f"{num_waiting_tasks} waiting. "
                        f"{num_completed_tasks} completed. "
                        f"{num_failed_tasks} failed. "
                        f"{num_upstream_failed_tasks} "
                        f"upstream failed. "
                        f"{num_failed_test_tasks} test failed. "
                        f"Elapsed time: {timedelta(seconds=int(time.time() - start_time))}",
                        end="\r",
                    )
                await asyncio.sleep(_constants._EVENT_LOOP_SLEEP)
                continue
            return tasks
    except:
        pass
    finally:
        for task in futures:
            task.cancel()
        await asyncio.gather(*futures)
        w_print("")
        return tasks


def sql_scheduler(
    stage: Optional[Literal["dev", "prod"]] = None,
    dev_schema: Optional[str] = None,
    targets: Optional[List[str]] = None,
    dependencies: bool = False,
    check: bool = False,
):
    try:
        tasks = asyncio.run(
            execute(
                stage=stage,
                dev_schema=dev_schema,
                targets=targets,
                dependencies=dependencies,
                check=check,
            )
        )
    except:
        w_print("")
        sys.exit(1)

    failed_task_ids = []
    test_failed_tasks = []
    upstream_failed_task_ids = []
    for task in tasks:
        if task.status == SQLTaskStatus.FAILED:
            failed_task_ids.append(task.task_id.lower())
        if task.status == SQLTaskStatus.TEST_FAILED:
            test_failed_tasks.append(task)
        if task.status == SQLTaskStatus.UPSTREAM_FAILED:
            upstream_failed_task_ids.append(task.task_id.lower())

    if (
        len(failed_task_ids) + len(test_failed_tasks) + len(upstream_failed_task_ids)
        == 0
    ):
        w_print(f"Execution Complete. All {len(tasks)} tasks run successfully.")
        return

    w_print(f"Execution Complete. {len(failed_task_ids)} tasks failed:")
    for failed_task_id in sorted(failed_task_ids):
        print(f" - {failed_task_id}")

    if len(test_failed_tasks) > 0:
        print(f"Tasks failed tests:")
        for test_failed_task in test_failed_tasks:
            print(
                f" - {test_failed_task.task_id}: {','.join(test_failed_task.failed_tests)}"
            )

    if len(upstream_failed_task_ids) > 0:
        print(f"Tasks not run because of upstream failures:")
        for upstream_failed_task_id in sorted(upstream_failed_task_ids):
            print(f" - {upstream_failed_task_id}")

    sys.exit(1)


def main():
    stage, dev_schema, targets, dependencies, check = _parse_arguments()
    sql_scheduler(
        stage=stage,
        dev_schema=dev_schema,
        targets=targets,
        dependencies=dependencies,
        check=check,
    )


if __name__ == "__main__":
    main()
