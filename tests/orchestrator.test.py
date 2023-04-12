import contextlib
import os
import sys
import unittest
from typing import Dict, List, Tuple
from uuid import uuid4

from sql_scheduler._helpers import Logger
from sql_scheduler.exceptions import *
from sql_scheduler.orchestrator import SQLOrchestrator
from sql_scheduler.sql_task import SQLTask, SQLTaskStatus

logger = Logger()

dummy_dir = os.path.join(f"/tmp/sql_scheduler_test")

dir_path = os.path.dirname(os.path.realpath(__file__))
ddl_path = os.path.join(dir_path, "sql_orchestrator_test_cases", "ddl")
insert_path = os.path.join(dir_path, "sql_orchestrator_test_cases", "insert")


class DummyFile(object):
    def write(self, x):
        pass

    def flush(self):
        pass


@contextlib.contextmanager
def nostdout():
    save_stdout = sys.stdout
    sys.stdout = DummyFile()
    yield
    sys.stdout = save_stdout


def generate_sql_tasks(
    task_ids: List[str],
) -> Tuple[Dict[str, SQLTask], List[SQLTask], List[str]]:
    d_tasks = {
        task_id: SQLTask(
            ddl_directory=ddl_path,
            insert_directory=insert_path,
            task_id=task_id,
            stage="prod",
            dev_schema="dev_schema",
            dsn="",
            cache_duration=600,
            logger=Logger(),
        )
        for task_id in task_ids
    }
    for task in d_tasks.values():
        task.remove_second_class_dependencies(set(d_tasks.keys()))
    tasks = list(d_tasks.values())
    return d_tasks, tasks, task_ids


class TestSQLOrchestrator(unittest.TestCase):
    def test_initialize(self):
        with self.assertRaises(SQLSchedulerInvalidDDLDirectory):
            o = SQLOrchestrator("/temp/non_existent", dummy_dir, logger)

        with self.assertRaises(SQLSchedulerInvalidInsertDirectory):
            o = SQLOrchestrator(dummy_dir, "/temp/non_existent", logger)

        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        self.assertEqual(o.ddl_directory, dummy_dir)
        self.assertEqual(o.insert_directory, dummy_dir)

    def test_parse_tasks_empty(self):
        random_dir = os.path.join(dir_path, uuid4().hex)
        os.makedirs(random_dir)
        o = SQLOrchestrator(random_dir, random_dir, logger)
        tasks = o._parse_tasks("dev", "dev_schema", "", 600, False)
        os.removedirs(random_dir)
        self.assertListEqual([], tasks)

    def test_parse_tasks_no_sql(self):
        random_dir = os.path.join(dir_path, uuid4().hex)
        os.makedirs(random_dir)
        with open(os.path.join(random_dir, "test.txt"), "w") as f:
            f.write("test")
        o = SQLOrchestrator(random_dir, random_dir, logger)
        tasks = o._parse_tasks("dev", "dev_schema", "", 600, False)
        os.remove(os.path.join(random_dir, "test.txt"))
        os.removedirs(random_dir)
        self.assertListEqual([], tasks)

    def test_parse_tasks_happy_case(self):
        random_dir = os.path.join(dir_path, uuid4().hex)
        os.makedirs(random_dir)
        file_1 = os.path.join(random_dir, "schema.a.sql")
        file_2 = os.path.join(random_dir, "schema.b.sql")
        with open(file_1, "w") as f:
            f.write("a")
        with open(file_2, "w") as f:
            f.write("b")
        o = SQLOrchestrator(random_dir, random_dir, logger)
        tasks = o._parse_tasks("dev", "dev_schema", "", 600, False)

        self.assertEqual(2, len(tasks))

        self.assertSetEqual({"a", "b"}, {task.get_insert() for task in tasks})
        self.assertSetEqual({"schema.a", "schema.b"}, {task.task_id for task in tasks})

        os.remove(file_1)
        os.remove(file_2)
        os.removedirs(random_dir)

    def test_should_start_task_single(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        s = o._should_start_task(d_tasks["public.a"])
        self.assertTrue(s)

    def test_should_start_task_non_dependent(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        s = o._should_start_task(d_tasks["public.a"])
        self.assertTrue(s)
        s = o._should_start_task(d_tasks["public.b"])
        self.assertFalse(s)
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = o._should_start_task(d_tasks["public.b"])
        self.assertTrue(s)

    def test_should_start_task_upstream_failed(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = o._should_start_task(d_tasks["public.b"])
        self.assertTrue(s)
        d_tasks["public.a"].status = SQLTaskStatus.UPSTREAM_FAILED
        with nostdout():
            s = o._should_start_task(d_tasks["public.b"])
        self.assertFalse(s)
        self.assertEqual(SQLTaskStatus.UPSTREAM_FAILED, d_tasks["public.b"].status)

    def test_should_start_task_failed(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = o._should_start_task(d_tasks["public.b"])
        self.assertTrue(s)
        d_tasks["public.a"].status = SQLTaskStatus.FAILED
        with nostdout():
            s = o._should_start_task(d_tasks["public.b"])
        self.assertFalse(s)
        self.assertEqual(SQLTaskStatus.UPSTREAM_FAILED, d_tasks["public.b"].status)

    def test_should_start_task_test_failed(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = o._should_start_task(d_tasks["public.b"])
        self.assertTrue(s)
        d_tasks["public.a"].status = SQLTaskStatus.TEST_FAILED
        with nostdout():
            s = o._should_start_task(d_tasks["public.b"])
        self.assertFalse(s)
        self.assertEqual(SQLTaskStatus.UPSTREAM_FAILED, d_tasks["public.b"].status)

    def test_should_start_task_self_fail(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        d_tasks["public.a"].status = SQLTaskStatus.FAILED
        s = o._should_start_task(d_tasks["public.a"])
        self.assertFalse(s)

    def test_should_start_task_waiting(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = o._should_start_task(d_tasks["public.b"])
        self.assertTrue(s)
        d_tasks["public.a"].status = SQLTaskStatus.WAITING
        s = o._should_start_task(d_tasks["public.b"])
        self.assertFalse(s)
        self.assertEqual(SQLTaskStatus.WAITING, d_tasks["public.b"].status)

    def test_should_start_task_running(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = o._should_start_task(d_tasks["public.b"])
        self.assertTrue(s)
        d_tasks["public.a"].status = SQLTaskStatus.RUNNING
        s = o._should_start_task(d_tasks["public.b"])
        self.assertFalse(s)
        self.assertEqual(SQLTaskStatus.WAITING, d_tasks["public.b"].status)

    def test_should_start_task_self_queued(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        d_tasks["public.a"].status = SQLTaskStatus.QUEUED
        s = o._should_start_task(d_tasks["public.a"])
        self.assertFalse(s)

    def test_should_start_task_queued(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = o._should_start_task(d_tasks["public.b"])
        self.assertTrue(s)
        d_tasks["public.a"].status = SQLTaskStatus.QUEUED
        s = o._should_start_task(d_tasks["public.b"])
        self.assertFalse(s)
        self.assertEqual(SQLTaskStatus.WAITING, d_tasks["public.b"].status)

    def test_circular_empty(self):
        d_tasks, tasks, task_ids = generate_sql_tasks([])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        c = o._circular_check()
        self.assertFalse(c)

    def test_circular_no_circle(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.a", "public.b", "public.c", "public.d"]
        )
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        c = o._circular_check()
        self.assertFalse(c)

    def test_circular_linked_circle(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.y", "public.z"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        with nostdout():
            c = o._circular_check()
        self.assertTrue(c)

    def test_circular_linked_circle_with_other_tasks(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.a", "public.b", "public.c", "public.d", "public.y", "public.z"]
        )
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        with nostdout():
            c = o._circular_check()
        self.assertTrue(c)

    def test_circular_seperated_circle(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.circular_1", "public.circular_2", "public.circular_3"]
        )
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        with nostdout():
            c = o._circular_check()
        self.assertTrue(c)

    def test_circular_partial_separated_circle(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.circular_1", "public.circular_3"]
        )
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        with nostdout():
            c = o._circular_check()
        self.assertFalse(c)

    def test_tasks_subset_no_target(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.a", "public.b", "public.c"]
        )
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks

        o._tasks_subset(None, False)
        self.assertEqual(3, len(o.tasks))

    def test_tasks_subset_single_target(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.a", "public.b", "public.c"]
        )
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks

        o._tasks_subset(["public.a"], False)
        self.assertEqual(1, len(o.tasks))
        self.assertEqual("public.a", o.tasks[0].task_id)

    def test_tasks_subset_single_target_dependencies(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.a", "public.b", "public.c"]
        )
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        with nostdout():
            o._tasks_subset(["public.b"], True)
        self.assertEqual(2, len(o.tasks))
        self.assertSetEqual(
            {"public.a", "public.b"}, {task.task_id for task in o.tasks}
        )

    def test_tasks_nonexistent_target(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.a", "public.b", "public.c"]
        )
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        with self.assertRaises(SQLSchedulerTargetNotFound):
            with nostdout():
                o._tasks_subset(["public.d"], True)
        with self.assertRaises(SQLSchedulerTargetNotFound):
            o._tasks_subset(["public.d"], False)

    def test_get_task_parents_no_dependencies(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks
        parents = o._get_task_parents(d_tasks["public.a"])
        self.assertSetEqual({d_tasks["public.a"]}, parents)

    def test_get_task_parents_one_dependency(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks

        parents = o._get_task_parents(d_tasks["public.a"])
        self.assertSetEqual({d_tasks["public.a"]}, parents)
        parents = o._get_task_parents(d_tasks["public.b"])
        self.assertSetEqual(set(tasks), parents)

    def test_get_task_parents_not_parent(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.a", "public.b", "public.c"]
        )
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks

        parents = o._get_task_parents(d_tasks["public.a"])
        self.assertSetEqual({d_tasks["public.a"]}, parents)
        parents = o._get_task_parents(d_tasks["public.b"])
        self.assertSetEqual({d_tasks["public.a"], d_tasks["public.b"]}, parents)
        parents = o._get_task_parents(d_tasks["public.c"])
        self.assertSetEqual({d_tasks["public.c"]}, parents)

    def test_get_task_dependencies_two_dependencies(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.a", "public.b", "public.c", "public.d"]
        )
        o = SQLOrchestrator(dummy_dir, dummy_dir, logger)
        o.tasks = tasks

        parents = o._get_task_parents(d_tasks["public.a"])
        self.assertSetEqual({d_tasks["public.a"]}, parents)
        parents = o._get_task_parents(d_tasks["public.b"])
        self.assertSetEqual({d_tasks["public.a"], d_tasks["public.b"]}, parents)
        parents = o._get_task_parents(d_tasks["public.c"])
        self.assertSetEqual({d_tasks["public.c"]}, parents)
        parents = o._get_task_parents(d_tasks["public.d"])
        self.assertSetEqual(
            {d_tasks["public.a"], d_tasks["public.b"], d_tasks["public.d"]}, parents
        )


if __name__ == "__main__":
    os.makedirs(dummy_dir, exist_ok=True)
    unittest.main()
    os.remove(dummy_dir)
