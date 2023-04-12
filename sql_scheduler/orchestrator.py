import asyncio
import os
import time
from datetime import datetime, timedelta
from typing import List, Literal, Optional, Set, Tuple

from ._constants import (
    _BASE_CACHE_DURATION,
    _BASE_CONCURRENCY,
    _EVENT_LOOP_SLEEP,
    _STAGE_DEV,
    _STAGE_PROD,
    _TASK_FILE_ENDING,
)
from ._helpers import Logger, construct_table
from .exceptions import *
from .sql_task import SQLTask, SQLTaskStatus


class SQLOrchestrator:
    logger: Logger
    ddl_directory: str
    insert_directory: str
    tasks: List[SQLTask]
    task_ids: Set[str]
    task_queue: List[SQLTask]

    start_time: Optional[float] = None

    def __init__(
        self, ddl_directory: str, insert_directory: str, logger: Logger = Logger()
    ):
        self.ddl_directory = ddl_directory
        self.insert_directory = insert_directory
        self.logger = logger
        self._verify()

        self.tasks = []
        self.task_ids = set()
        self.task_queue = []

    def _verify(self):
        if not os.path.exists(self.ddl_directory) or not os.path.isdir(
            self.ddl_directory
        ):
            raise SQLSchedulerInvalidDDLDirectory()

        if not os.path.exists(self.insert_directory) or not os.path.isdir(
            self.insert_directory
        ):
            raise SQLSchedulerInvalidInsertDirectory()

    async def execute(
        self,
        dsn: str,
        incremental_interval: Tuple[datetime, datetime],
        targets: Optional[List[str]] = None,
        dependencies: bool = False,
        stage: Literal["prod", "dev"] = _STAGE_PROD,
        dev_schema: Optional[str] = None,
        cache_duration: int = _BASE_CACHE_DURATION,
        refill: bool = False,
        no_cache: bool = False,
        concurrency: int = _BASE_CONCURRENCY,
        simple_output: bool = False,
        verbose: bool = False,
    ):
        if stage == _STAGE_DEV and dev_schema is None:
            raise SQLSchedulerNoDevSchema()

        self.tasks = self._parse_tasks(
            stage=stage,
            dev_schema=dev_schema,
            dsn=dsn,
            cache_duration=cache_duration,
            verbose=verbose,
        )
        circular = self._circular_check()
        if circular:
            raise SQLSchedulerCycleFound()

        self._tasks_subset(targets, dependencies)
        self.task_ids = {task.task_id.lower() for task in self.tasks}
        if any([task.incremental for task in self.tasks]):
            start, end = incremental_interval
            self.logger.out(
                f'Incremental tasks will be run with the interval {start.strftime("%Y-%m-%d %H:%M:%S.%f")} -> {end.strftime("%Y-%m-%d %H:%M:%S.%f")}'
            )
        await self._execution_loop(
            incremental_interval=incremental_interval,
            refill=refill,
            no_cache=no_cache,
            concurrency=concurrency,
            simple_output=simple_output,
        )
        return self._post_execution()

    async def _execution_loop(
        self,
        incremental_interval: Tuple[datetime, datetime],
        refill: bool,
        no_cache: bool,
        concurrency: int,
        simple_output: bool,
    ):
        start_time = time.time()
        futures: List[asyncio.Future] = []
        try:
            while True:
                task_status_counts = self._task_status_counts()
                if (
                    task_status_counts[SQLTaskStatus.WAITING]
                    + task_status_counts[SQLTaskStatus.QUEUED]
                    + task_status_counts[SQLTaskStatus.RUNNING]
                    == 0
                ):
                    break
                for task in self.tasks:
                    if self._should_start_task(task):
                        task.status = SQLTaskStatus.QUEUED
                        self.task_queue.append(task)

                if task_status_counts[SQLTaskStatus.RUNNING] < concurrency:
                    tasks_to_start = self.task_queue[
                        0 : concurrency - task_status_counts[SQLTaskStatus.RUNNING]
                    ]
                    self.task_queue = self.task_queue[
                        concurrency - task_status_counts[SQLTaskStatus.RUNNING] :
                    ]
                    for task in tasks_to_start:
                        self.logger.out(
                            f"Scheduling task {task.task_id.lower()} for execution"
                        )
                        futures.append(
                            asyncio.create_task(
                                task.execute(
                                    self.task_ids,
                                    incremental_interval=incremental_interval,
                                    refill=refill,
                                    no_cache=no_cache,
                                ),
                                name=task.task_id.lower(),
                            )
                        )

                if not simple_output:
                    self.logger.out(
                        f"{task_status_counts[SQLTaskStatus.RUNNING]} running. "
                        f"{task_status_counts[SQLTaskStatus.WAITING]} waiting. "
                        f"{task_status_counts[SQLTaskStatus.QUEUED]} queued. "
                        f"{task_status_counts[SQLTaskStatus.SUCCESS]} completed. "
                        f"{task_status_counts[SQLTaskStatus.FAILED]} failed. "
                        f"{task_status_counts[SQLTaskStatus.UPSTREAM_FAILED]} "
                        f"upstream failed. "
                        f"{task_status_counts[SQLTaskStatus.TEST_FAILED]} test failed. "
                        f"Elapsed time: {timedelta(seconds=int(time.time() - start_time))}",
                        persist=False,
                    )

                await asyncio.sleep(_EVENT_LOOP_SLEEP)
        except Exception as e:
            self.logger.out(str(e))

    def _post_execution(self):
        failed_task_ids = []
        test_failed_tasks = []
        upstream_failed_task_ids = []
        for task in self.tasks:
            if task.status == SQLTaskStatus.FAILED:
                failed_task_ids.append(task.task_id.lower())
            if task.status == SQLTaskStatus.TEST_FAILED:
                test_failed_tasks.append(task)
            if task.status == SQLTaskStatus.UPSTREAM_FAILED:
                upstream_failed_task_ids.append(task.task_id.lower())

        self.logger.out(f"Execution Complete.")
        self.logger.out(
            construct_table(
                ["task_id", "script duration (s)", "test duration (s)"],
                [
                    (
                        task.task_id.lower(),
                        str(
                            round(
                                task.script_duration
                                if task.script_duration is not None
                                else -1,
                                1,
                            )
                        ),
                        str(
                            round(
                                task.test_duration
                                if task.test_duration is not None
                                else -1,
                                1,
                            )
                        ),
                    )
                    for task in sorted(
                        filter(
                            lambda task: task.status
                            not in (
                                SQLTaskStatus.WAITING,
                                SQLTaskStatus.UPSTREAM_FAILED,
                            ),
                            self.tasks,
                        ),
                        key=lambda task: task.start_timestamp,
                    )
                ],
            )
        )
        if (
            len(failed_task_ids)
            + len(test_failed_tasks)
            + len(upstream_failed_task_ids)
            == 0
        ):
            print(f"All {len(self.tasks)} tasks run successfully.")
            return 0

        print(f"{len(failed_task_ids)} tasks failed:")
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

        return (
            len(upstream_failed_task_ids)
            + len(failed_task_ids)
            + len(test_failed_tasks)
        )

    def _parse_tasks(
        self,
        stage: Literal["prod", "dev"],
        dev_schema: Optional[str],
        dsn: str,
        cache_duration: int,
        verbose: bool,
    ) -> List[SQLTask]:
        tasks = [
            SQLTask(
                self.ddl_directory,
                self.insert_directory,
                filename[: -1 * len(_TASK_FILE_ENDING)],
                stage=stage,
                dev_schema=dev_schema,
                dsn=dsn,
                cache_duration=cache_duration,
                logger=self.logger,
                verbose=verbose,
            )
            for filename in os.listdir(self.insert_directory)
            if filename[-1 * len(_TASK_FILE_ENDING) :] == _TASK_FILE_ENDING
        ]
        task_ids = {task.task_id.lower() for task in tasks}
        for task in tasks:
            task.remove_second_class_dependencies(task_ids)
        return tasks

    def _circular_check(
        self,
    ) -> bool:
        """Return True if Cycle found."""
        result = 0
        for task in self.tasks:
            result += self._recursive_circular_check(task, history=[])
        return result > 0

    def _recursive_circular_check(self, task: SQLTask, history: List[str] = []) -> int:
        """Return 1 if a circular dependency was found"""
        if task.task_id.lower() in history:
            self.logger.out(
                f'Circular dependency found between tasks ({",".join(history)}).'
            )
            return 1
        results = 0
        for dependency in task.dependencies:
            parent_task = [
                _task
                for _task in self.tasks
                if _task.task_id.lower() == dependency.lower()
            ]
            if len(parent_task) != 1:
                raise SQLSchedulerInvalidDependency(
                    f"task {task.task_id} has a non-existent dependency {dependency}."
                )
            results += self._recursive_circular_check(
                parent_task[0], [*history, task.task_id.lower()]
            )

        return results

    def _get_task_parents(self, task: SQLTask) -> Set[SQLTask]:
        if len(task.dependencies) == 0:
            return {task}
        parents = {task}
        for dependency in task.dependencies:
            parent_task = [
                _task
                for _task in self.tasks
                if _task.task_id.lower() == dependency.lower()
            ]
            if len(parent_task) != 1:
                raise SQLSchedulerInvalidDependency(
                    f"task {task.task_id} has a non-existent dependency {dependency}."
                )

            parents.update(self._get_task_parents(parent_task[0]))
        return parents

    def _tasks_subset(self, targets: Optional[List[str]], dependencies: bool):
        if targets is None:
            return

        if dependencies:
            self.logger.out(f"Identifying upstream dependencies of {targets}...")
            subset_tasks: Set[SQLTask] = set()
            for target in targets:
                target_task = [_task for _task in self.tasks if _task.task_id == target]
                if len(target_task) != 1:
                    raise SQLSchedulerTargetNotFound(target)
                subset_tasks.update(self._get_task_parents(target_task[0]))
            self.logger.out(
                f"Found {len(subset_tasks) - len(targets)} tasks in upstream dependencies."
            )
            self.tasks = list(subset_tasks)
        else:
            self.tasks = [task for task in self.tasks if task.task_id in targets]
            if len(self.tasks) != len(targets):
                raise SQLSchedulerTargetNotFound(
                    set(targets) - {task.task_id for task in self.tasks}
                )

    def _should_start_task(self, task: SQLTask) -> bool:
        if task.status != SQLTaskStatus.WAITING:
            return False
        task_statuses = {task.task_id.lower(): task.status for task in self.tasks}
        for task_id in task.dependencies:
            task_status = task_statuses.get(task_id)
            if task_status in [
                SQLTaskStatus.UPSTREAM_FAILED,
                SQLTaskStatus.FAILED,
                SQLTaskStatus.TEST_FAILED,
            ]:
                self.logger.out(
                    f"Marking task {task.task_id.lower()} as UPSTREAM_FAILED due to {task_id} ({task_statuses[task_id].name})"
                )
                task.status = SQLTaskStatus.UPSTREAM_FAILED
                return False
            if task_status in [
                SQLTaskStatus.WAITING,
                SQLTaskStatus.QUEUED,
                SQLTaskStatus.RUNNING,
            ]:
                return False
        return True

    def _task_status_counts(self):
        task_counts = {
            SQLTaskStatus.WAITING: 0,
            SQLTaskStatus.QUEUED: 0,
            SQLTaskStatus.RUNNING: 0,
            SQLTaskStatus.SUCCESS: 0,
            SQLTaskStatus.FAILED: 0,
            SQLTaskStatus.UPSTREAM_FAILED: 0,
            SQLTaskStatus.TEST_FAILED: 0,
        }
        for task in self.tasks:
            task_counts[task.status] += 1
        return task_counts
