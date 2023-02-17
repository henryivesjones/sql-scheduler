import os
import unittest

from sql_scheduler.sql_task import SQLTask

dir_path = os.path.dirname(os.path.realpath(__file__))
ddl_path = os.path.join(dir_path, "sql_task_test_cases", "ddl")
insert_path = os.path.join(dir_path, "sql_task_test_cases", "insert")


class TestParseDependencies(unittest.TestCase):
    def test_empty_script(self):
        task = SQLTask(
            ddl_path, insert_path, "public.empty", "prod", "dev_schema", "", 600, False
        )
        dependencies = task._parse_dependencies()
        self.assertSetEqual(set(), dependencies)

    def test_no_dependencies(self):
        task = SQLTask(
            ddl_path,
            insert_path,
            "public.no_deps",
            "prod",
            "dev_schema",
            "",
            600,
            False,
        )
        dependencies = task._parse_dependencies()
        self.assertSetEqual(set(), dependencies)

    def test_single_from(self):
        task = SQLTask(
            ddl_path,
            insert_path,
            "public.single_from",
            "prod",
            "dev_schema",
            "",
            600,
            False,
        )
        dependencies = task._parse_dependencies()
        self.assertSetEqual({"public.table_a"}, dependencies)

    def test_multiple_from(self):
        task = SQLTask(
            ddl_path,
            insert_path,
            "public.multiple_from",
            "prod",
            "dev_schema",
            "",
            600,
            False,
        )
        dependencies = task._parse_dependencies()
        self.assertSetEqual({"public.table_a", "public.table_b"}, dependencies)

    def test_join_types(self):
        task = SQLTask(
            ddl_path,
            insert_path,
            "public.joins",
            "prod",
            "dev_schema",
            "",
            600,
            False,
        )
        dependencies = task._parse_dependencies()
        self.assertSetEqual(
            {
                "public.table_a",
                "public.table_b",
                "public.table_c",
                "public.table_d",
                "public.table_e",
                "public.table_f",
            },
            dependencies,
        )

    def test_caps_tests(self):
        task = SQLTask(
            ddl_path,
            insert_path,
            "public.caps",
            "prod",
            "dev_schema",
            "",
            600,
            False,
        )
        dependencies = task._parse_dependencies()
        self.assertSetEqual(
            {
                "public.table_a",
                "public.table_b",
                "public.table_c",
                "public.table_d",
                "public.table_e",
                "public.table_f",
            },
            dependencies,
        )

    def test_spacing_tests(self):
        task = SQLTask(
            ddl_path,
            insert_path,
            "public.spacing",
            "prod",
            "dev_schema",
            "",
            600,
            False,
        )
        dependencies = task._parse_dependencies()
        self.assertSetEqual(
            {
                "public.table_a",
                "public.table_b",
                "public.table_c",
                "public.table_d",
                "public.table_e",
                "public.table_f",
            },
            dependencies,
        )


if __name__ == "__main__":
    unittest.main()
