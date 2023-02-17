import os
import unittest

from sql_scheduler.sql_task import SQLTask

dir_path = os.path.dirname(os.path.realpath(__file__))
ddl_path = os.path.join(dir_path, "sql_task_test_cases", "ddl")
insert_path = os.path.join(dir_path, "sql_task_test_cases", "insert")

replace_for_dev_test_path = os.path.join(
    dir_path, "sql_task_replace_for_dev_cases", "test"
)
replace_for_dev_solution_path = os.path.join(
    dir_path, "sql_task_replace_for_dev_cases", "solution"
)


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


class TestReplaceForDev(unittest.TestCase):
    def test_no_other_tasks(self):
        task = SQLTask(
            replace_for_dev_test_path,
            replace_for_dev_test_path,
            "public.exhaustive_keywords",
            "dev",
            "dev_schema",
            "",
            600,
            False,
        )
        replaced_query = task._replace_for_dev(task.get_insert(), set())
        with open(
            os.path.join(
                replace_for_dev_solution_path, "public.exhaustive_keywords_no_deps.sql"
            ),
            "r",
        ) as solution_file:
            self.assertEqual(solution_file.read(), replaced_query)

    def test_one_task(self):
        task = SQLTask(
            replace_for_dev_test_path,
            replace_for_dev_test_path,
            "public.exhaustive_keywords",
            "dev",
            "dev_schema",
            "",
            600,
            False,
        )
        replaced_query = task._replace_for_dev(task.get_insert(), {"public.table_b"})
        with open(
            os.path.join(
                replace_for_dev_solution_path, "public.exhaustive_keywords_table_b.sql"
            ),
            "r",
        ) as solution_file:
            self.assertEqual(solution_file.read(), replaced_query)

    def test_all_referenced_tasks(self):
        task = SQLTask(
            replace_for_dev_test_path,
            replace_for_dev_test_path,
            "public.exhaustive_keywords",
            "dev",
            "dev_schema",
            "",
            600,
            False,
        )
        replaced_query = task._replace_for_dev(
            task.get_insert(),
            {"public.table_b", "public.table_c"},
        )
        with open(
            os.path.join(
                replace_for_dev_solution_path,
                "public.exhaustive_keywords_table_b_c.sql",
            ),
            "r",
        ) as solution_file:
            self.assertEqual(solution_file.read(), replaced_query)

    def test_all_referenced_tasks_wih_extra(self):
        task = SQLTask(
            replace_for_dev_test_path,
            replace_for_dev_test_path,
            "public.exhaustive_keywords",
            "dev",
            "dev_schema",
            "",
            600,
            False,
        )
        replaced_query = task._replace_for_dev(
            task.get_insert(),
            {"public.table_b", "public.table_c", "public.table_z"},
        )
        with open(
            os.path.join(
                replace_for_dev_solution_path,
                "public.exhaustive_keywords_table_b_c.sql",
            ),
            "r",
        ) as solution_file:
            self.assertEqual(solution_file.read(), replaced_query)

    def test_quotes(self):
        self.maxDiff = None
        task = SQLTask(
            replace_for_dev_test_path,
            replace_for_dev_test_path,
            "public.exhaustive_keywords_quotes",
            "dev",
            "dev_schema",
            "",
            600,
            False,
        )
        replaced_query = task._replace_for_dev(
            task.get_insert(),
            {"public.table_b", "public.table_c"},
        )
        with open(
            os.path.join(
                replace_for_dev_solution_path,
                "public.exhaustive_keywords_quotes_table_b_c.sql",
            ),
            "r",
        ) as solution_file:
            self.assertEqual(solution_file.read(), replaced_query)

    def test_spacing(self):
        self.maxDiff = None
        task = SQLTask(
            replace_for_dev_test_path,
            replace_for_dev_test_path,
            "public.exhaustive_keywords_spacing",
            "dev",
            "dev_schema",
            "",
            600,
            False,
        )
        replaced_query = task._replace_for_dev(
            task.get_insert(),
            {"public.table_b", "public.table_c"},
        )
        with open(
            os.path.join(
                replace_for_dev_solution_path,
                "public.exhaustive_keywords_spacing_table_b_c.sql",
            ),
            "r",
        ) as solution_file:
            self.assertEqual(solution_file.read(), replaced_query)


if __name__ == "__main__":
    unittest.main()
