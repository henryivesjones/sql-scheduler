import contextlib
import os
import sys
import unittest
from typing import Dict, List, Tuple
from uuid import uuid4

from sql_scheduler.sql_scheduler import (
    _circular_check,
    _get_task_parents,
    _parse_tasks,
    _should_start_task,
)
from sql_scheduler.sql_task import SQLTask, SQLTaskStatus


class DummyFile(object):
    def write(self, x):
        pass


@contextlib.contextmanager
def nostdout():
    save_stdout = sys.stdout
    sys.stdout = DummyFile()
    yield
    sys.stdout = save_stdout


dir_path = os.path.dirname(os.path.realpath(__file__))
ddl_path = os.path.join(dir_path, "test_cases", "ddl")
insert_path = os.path.join(dir_path, "test_cases", "insert")


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
        )
        for task_id in task_ids
    }
    for task in d_tasks.values():
        task.remove_second_class_dependencies(set(d_tasks.keys()))
    tasks = list(d_tasks.values())
    return d_tasks, tasks, task_ids


class TestGetTaskParents(unittest.TestCase):
    def test_no_deps(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a"])
        parents = _get_task_parents(d_tasks["public.a"], list(d_tasks.values()))
        self.assertSetEqual({d_tasks["public.a"]}, parents)

    def test_one_dep(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])

        parents = _get_task_parents(d_tasks["public.a"], tasks)
        self.assertSetEqual({d_tasks["public.a"]}, parents)
        parents = _get_task_parents(d_tasks["public.b"], tasks)
        self.assertSetEqual(set(tasks), parents)

    def test_task_not_parent(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.a", "public.b", "public.c"]
        )

        parents = _get_task_parents(d_tasks["public.a"], tasks)
        self.assertSetEqual({d_tasks["public.a"]}, parents)
        parents = _get_task_parents(d_tasks["public.b"], tasks)
        self.assertSetEqual({d_tasks["public.a"], d_tasks["public.b"]}, parents)
        parents = _get_task_parents(d_tasks["public.c"], tasks)
        self.assertSetEqual({d_tasks["public.c"]}, parents)

    def test_two_dep(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.a", "public.b", "public.c", "public.d"]
        )

        parents = _get_task_parents(d_tasks["public.a"], tasks)
        self.assertSetEqual({d_tasks["public.a"]}, parents)
        parents = _get_task_parents(d_tasks["public.b"], tasks)
        self.assertSetEqual({d_tasks["public.a"], d_tasks["public.b"]}, parents)
        parents = _get_task_parents(d_tasks["public.c"], tasks)
        self.assertSetEqual({d_tasks["public.c"]}, parents)
        parents = _get_task_parents(d_tasks["public.d"], tasks)
        self.assertSetEqual(
            {d_tasks["public.a"], d_tasks["public.b"], d_tasks["public.d"]}, parents
        )


class TestCircularCheck(unittest.TestCase):
    def test_empty(self):
        c = _circular_check([])
        self.assertFalse(c)
        pass

    def test_no_circle(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.a", "public.b", "public.c", "public.d"]
        )
        c = _circular_check(tasks)
        self.assertFalse(c)
        pass

    def test_linked_circle(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.y", "public.z"])
        with nostdout():
            c = _circular_check(tasks)
        self.assertTrue(c)
        pass

    def test_linked_circle_with_other_tasks(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.a", "public.b", "public.c", "public.d", "public.y", "public.z"]
        )
        with nostdout():
            c = _circular_check(tasks)
        self.assertTrue(c)

    def test_seperated_circle(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.circular_1", "public.circular_2", "public.circular_3"]
        )
        with nostdout():
            c = _circular_check(tasks)
        self.assertTrue(c)
        pass

    def test_partial_seperated_circle(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(
            ["public.circular_1", "public.circular_3"]
        )
        with nostdout():
            c = _circular_check(tasks)
        self.assertFalse(c)
        pass


class TestParseTasks(unittest.TestCase):
    def test_empty_dir(self):
        random_dir = os.path.join(dir_path, uuid4().hex)
        os.makedirs(random_dir)
        tasks = _parse_tasks(random_dir, random_dir, "dev", "dev_schema", "")
        os.removedirs(random_dir)
        self.assertListEqual([], tasks)

    def test_no_sql(self):
        random_dir = os.path.join(dir_path, uuid4().hex)
        os.makedirs(random_dir)
        with open(os.path.join(random_dir, "test.txt"), "w") as f:
            f.write("test")
        tasks = _parse_tasks(random_dir, random_dir, "dev", "dev_schema", "")
        os.remove(os.path.join(random_dir, "test.txt"))
        os.removedirs(random_dir)
        self.assertListEqual([], tasks)

    def test_good_case(self):
        random_dir = os.path.join(dir_path, uuid4().hex)
        os.makedirs(random_dir)
        file_1 = os.path.join(random_dir, "schema.a.sql")
        file_2 = os.path.join(random_dir, "schema.b.sql")
        with open(file_1, "w") as f:
            f.write("a")
        with open(file_2, "w") as f:
            f.write("b")
        tasks = _parse_tasks(random_dir, random_dir, "dev", "dev_schema", "")

        self.assertEqual(2, len(tasks))

        self.assertSetEqual({"a", "b"}, {task.get_insert() for task in tasks})
        self.assertSetEqual({"schema.a", "schema.b"}, {task.task_id for task in tasks})

        os.remove(file_1)
        os.remove(file_2)
        os.removedirs(random_dir)


class TestShouldStartTask(unittest.TestCase):
    def test_single_task(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a"])
        s = _should_start_task(d_tasks["public.a"], tasks)
        self.assertTrue(s)

    def test_two_non_dependent_tasks(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.c"])
        s = _should_start_task(d_tasks["public.a"], tasks)
        self.assertTrue(s)
        s = _should_start_task(d_tasks["public.c"], tasks)
        self.assertTrue(s)

    def test_two_dependent_tasks(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        s = _should_start_task(d_tasks["public.a"], tasks)
        self.assertTrue(s)
        s = _should_start_task(d_tasks["public.b"], tasks)
        self.assertFalse(s)
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = _should_start_task(d_tasks["public.b"], tasks)
        self.assertTrue(s)

    def test_upstream_failed(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = _should_start_task(d_tasks["public.b"], tasks)
        self.assertTrue(s)
        d_tasks["public.a"].status = SQLTaskStatus.UPSTREAM_FAILED
        with nostdout():
            s = _should_start_task(d_tasks["public.b"], tasks)
        self.assertFalse(s)
        self.assertEqual(SQLTaskStatus.UPSTREAM_FAILED, d_tasks["public.b"].status)

    def test_failed(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = _should_start_task(d_tasks["public.b"], tasks)
        self.assertTrue(s)
        d_tasks["public.a"].status = SQLTaskStatus.FAILED
        with nostdout():
            s = _should_start_task(d_tasks["public.b"], tasks)
        self.assertFalse(s)
        self.assertEqual(SQLTaskStatus.UPSTREAM_FAILED, d_tasks["public.b"].status)

    def test_test_failed(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = _should_start_task(d_tasks["public.b"], tasks)
        self.assertTrue(s)
        d_tasks["public.a"].status = SQLTaskStatus.TEST_FAILED
        with nostdout():
            s = _should_start_task(d_tasks["public.b"], tasks)
        self.assertFalse(s)
        self.assertEqual(SQLTaskStatus.UPSTREAM_FAILED, d_tasks["public.b"].status)

    def test_self_fail(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a"])
        d_tasks["public.a"].status = SQLTaskStatus.FAILED
        s = _should_start_task(d_tasks["public.a"], tasks)
        self.assertFalse(s)

    def test_waiting(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = _should_start_task(d_tasks["public.b"], tasks)
        self.assertTrue(s)
        d_tasks["public.a"].status = SQLTaskStatus.WAITING
        s = _should_start_task(d_tasks["public.b"], tasks)
        self.assertFalse(s)
        self.assertEqual(SQLTaskStatus.WAITING, d_tasks["public.b"].status)

    def test_running(self):
        d_tasks, tasks, task_ids = generate_sql_tasks(["public.a", "public.b"])
        d_tasks["public.a"].status = SQLTaskStatus.SUCCESS
        s = _should_start_task(d_tasks["public.b"], tasks)
        self.assertTrue(s)
        d_tasks["public.a"].status = SQLTaskStatus.RUNNING
        s = _should_start_task(d_tasks["public.b"], tasks)
        self.assertFalse(s)
        self.assertEqual(SQLTaskStatus.WAITING, d_tasks["public.b"].status)


if __name__ == "__main__":
    unittest.main()
