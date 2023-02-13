# sql-scheduler
`sql-scheduler` allows you to easily run a suite of SQL scripts against a Postgres/Redshift database.

`sql-scheduler` works with pairs of `ddl` and `insert` scripts.

The `ddl` script takes the form:
```sql
DROP TABLE IF EXISTS "schema"."table";
CREATE TABLE "schema"."table" (
    column_a INT,
    column_b VARCHAR
);
```
The `insert` script takes the form:
```sql
INSERT INTO "schema"."table" (
    SELECT 1, ''
);
```
These scripts should be put into the correlating `ddl`/`insert` folders and should have identical names following the convention: `schema.table.sql` (I recommend using all lowercase).

In order for the dev schema replacement to work without issue, your table names should be unique. When run in the dev stage, the schemas are replaced with the given dev schema.

# Features:
 1. Automatic inter-script dependency management

Ensuring that script B which selects from the table created in script A runs after script A.

 2. Automatic schema replacement for development/staging workflows
 3. Concurrency

Scripts will be run concurrently if they don't depend on each other.

 4. Easy unit testing for table granularity, column nullability, and table relationships

# Quickstart:


## Installation
```bash
pip install sql-scheduler
```
## Configuration

### Required Environment Variables
 - `SQL_SCHEDULER_DDL_DIRECTORY`: An absolute path to the `ddl` directory. EX: `/home/ubuntu/sql/ddl/`
 - `SQL_SCHEDULER_INSERT_DIRECTORY`: An absolute path to the `insert` directory. EX: `/home/ubuntu/sql/insert/`
 - `SQL_SCHEDULER_DSN`: A DSN for connecting to your database in the form: `postgres://user:password@host:port/database?option=value`

### Optional Environment Variables
 - `SQL_SCHEDULER_STAGE`: The default stage (`prod`, `dev`) to run in. Can be overridden by the CLI flag `--dev` or `--prod`. When running in the dev stage a dev schema must be provided, either thru an Environment Variable, or a cli argument.
 - `SQL_SCHEDULER_DEV_SCHEMA`: The schema to replace with when run in the `dev` stage. Can be overridden by the CLI argument `--dev-schema`.
 - `SQL_SCHEDULER_SIMPLE_OUTPUT`: Simplify the output of this program by removing the status message. (If you are running `sql-scheduler` not in the CLI then you probably want to set this to `1`)

## Common Commands

### Running all scripts.
```bash
sql-scheduler
```
### Run a specific script.
```bash
sql-scheduler -t schema.table
```

### Run multiple specific scripts.
```bash
sql-scheduler -t schema.table -t schema.table2
```

### Run a specific script and all of its upstream dependencies.
```bash
sql-scheduler -t schema.table --dependencies
```

### Run a specific script in the dev stage.
```bash
sql-scheduler -t schema.table --dev
```

### Check for circular dependencies
```bash
sql-scheduler --check
```

# Non-CLI Usage
You may want to utilize `sql-scheduler` from within a python script instead of from the command line, for instance if you wanted to run it from within an airflow task. To do this simply import the `sql_scheduler` function which can be used as the entrypoint.
```python
from sql_scheduler.sql_scheduler import sql_scheduler

if __name__ == "__main__":
    sql_scheduler(
        stage="dev",
        dev_schema="dev_schema",
        targets=["prod_schema.table_a", "prod_schema.table_b"],
        dependencies=False,
        check=False,
    )
```
To remove the in progress status message, which doesn't always play nice with python logging, set the environment variable `SQL_SCHEDULER_SIMPLE_OUTPUT` to `1`.

# Tests
You can add tests to insert scripts which will make certain assertions about the data. Currently there are three options for tests: `granularity`, `not_null`, and `relationship`. To specify a test in a script simply add the test into a comment contained within the `insert` script. A failure of a test will stop downstream tasks from running.

## granularity
This test will assert that the granularity of the table is as expected. For example, lets say we have a table with three columns: `column_a`, `column_b`, and `column_c` and we expect that there should only be one row per unique combination of `column_a` and `column_b`. We can add this test assertion with the following:
```SQL
/*
granularity: column_a, column_b
*/
```
After populating this table, `sql-scheduler` will query the table and ensure that no more than one row exists for each unique combination of `column_a` and `column_b`.

## not_null
This test will assert that the given columns contain no null values. We can add this test assertion with the following:
```SQL
/*
not_null: column_a, column_b
*/
```

## relationship
This test will assert that all of the values in a given column are found within another column in another table. Keep in mind that this test is run after insertion, but before any downstream tasks are run so make sure to only reference upstream tables (or tables populated via other means). When running in `dev` stage any tables referenced will have their schemas swapped if they are upstream of the given table. Multiple relationships can be set.
```SQL
/*
relationship: column_a = schema.table.column_a
relationship: column_b = schema.table.column_b
*/
```

# Automatic inter-script dependency management
Before execution of a run, `sql-scheduler` parses all of the scripts found in the `ddl` and `insert` folders and identifies dependencies between scripts. It is able to do this by identifying tables referenced in `FROM` and `JOIN` statements within the `insert` query. During the execution of a run `sql-scheduler` ensures that any upstream dependencies have completed successfully before executing.

`sql-scheduler` will notify you of any circular dependencies found and exit. This can be checked without initiating a run with the flag `--check`.

# Automatic dev-schema replacement
A key feature of `sql-scheduler` is the ability to write scripts targeting a prod schema/s, and test these scripts inserting into a development schema. When combining this feature with the dependency inference this can make your development experience much much smoother. Not only will the insert/ddl schema change, but any references (from, join) inside the insert script will be switched to point to the dev schema IF that table will be populated by the current run of `sql-scheduler`.

I'll give you a few examples to better understand this functionality.
For these examples I will use the following insert scripts (I will omit the ddl statements as they aren't relevant here).
```SQL
INSERT INTO prod_schema.table_a (
    select column_a
        , column_b
        , column_c
        , column_d
    from raw_data_schema.table_x
);
```
```SQL
INSERT INTO prod_schema.table_b (
    select column_a
        , column_b
        , column_e
        , column_f
    from raw_data_schema.table_y
);
```
```SQL
INSERT INTO prod_schema.table_c (
    select column_a
        , column_b
        , sum(column_c) as s_column_c
    from prod_schema.table_a
    group by 1, 2
);
```
```SQL
INSERT INTO prod_schema.table_d (
    select c.column_a
        , c.column_b
        , (c.s_column_c + d.column_e) / d.column_f as a_new_metric
        , z.column_z
    from prod_schema.table_c c
    inner join prod_schema.table_b d on
        c.column_a = d.column_a
        and c.column_b = d.column_b
    inner join raw_data_schema.table_z z on
        c.column_a = z.column_a
    group by 1, 2
);
```
These four scripts will make up a dependency graph of:
```
table_a ─────►table_c─────►table_d
                              ▲
table_b───────────────────────┘
```
## Example #1
```
sql-scheduler -t prod_schema.table_a --dev --dev-schema dev_schema
```
This will run just the `prod_schema.table_a` script with dev replacement. Which would run the following statement:
```SQL
INSERT INTO dev_schema.table_a (
    select column_a
        , column_b
        , column_c
        , column_d
    from raw_data_schema.table_x
);
```
`raw_data_schema.table_x` is not changed at all, because it is not a table that is being modified by the current run.
## Example #2
```
sql-scheduler -t prod_schema.table_c --dev --dev-schema dev_schema --dependencies
```
This will run `prod_schema.table_c` and its upstream dependencies (`prod_schema.table_a`).

First the `prod_schema.table_a` with dev schema replacement:
```SQL
INSERT INTO dev_schema.table_a (
    select column_a
        , column_b
        , column_c
        , column_d
    from raw_data_schema.table_x
);
```
Then `prod_schema.table_c` will run both replacing its schema as well as the schema for its reference to `prod_schema.table_a`:
```SQL
INSERT INTO dev_schema.table_c (
    select column_a
        , column_b
        , sum(column_c) as s_column_c
    from dev_schema.table_a
    group by 1, 2
);
```

## Example #3
```
sql-scheduler -t prod_schema.table_c --dev --dev-schema dev_schema
```
This will run only `prod_schema.table_c`.

Because this run doesn't modify the table it references `prod_schema.table_a` that reference will be unchanged.
```SQL
INSERT INTO dev_schema.table_c (
    select column_a
        , column_b
        , sum(column_c) as s_column_c
    from prod_schema.table_a
    group by 1, 2
);
```

## Example #4
```
sql-scheduler -t prod_schema.table_d --dev --dev-schema dev_schema --dependencies
```
All upstream dependencies of `prod_schema.table_d` will be run.

Resulting in `prod_schema.table_a` and `prod_schema.table_b` running first concurrently:
```SQL
INSERT INTO dev_schema.table_a (
    select column_a
        , column_b
        , column_c
        , column_d
    from raw_data_schema.table_x
);
```
```SQL
INSERT INTO dev_schema.table_b (
    select column_a
        , column_b
        , column_e
        , column_f
    from raw_data_schema.table_y
);
```
Then `prod_schema.table_c` will be run with it's reference to `prod_schema.table_a` replaced with a reference to `dev_schema.table_a`
```SQL
INSERT INTO dev_schema.table_c (
    select column_a
        , column_b
        , sum(column_c) as s_column_c
    from dev_schema.table_a
    group by 1, 2
);
```
Then finally `prod_schema.table_d` will run with it's references to `prod_schema.table_c` and `prod_schema.table_b` replaced with `dev_schema.table_c` and `dev_schema.table_b` because they were both modified in this run. The reference to `raw_data_schema.table_z` is untouched because it was not modified by this run.
```SQL
INSERT INTO dev_schema.table_d (
    select c.column_a
        , c.column_b
        , (c.s_column_c + d.column_e) / d.column_f as a_new_metric
        , z.column_z
    from dev_schema.table_c c
    inner join dev_schema.table_b d on
        c.column_a = d.column_a
        and c.column_b = d.column_b
    inner join raw_data_schema.table_z z on
        c.column_a = z.column_a
    group by 1, 2
);
```
# How to organize/name scripts
`sql-scheduler` only cares about a few conventions when it comes to organizing and naming scripts:
1. A suite of scripts consists of two folders: a `ddl` folder, and an `insert` folder. The actual names of these directories and there placement relative to each other is not prescribed.
2. Any script in the `insert` folder must have an identically named script in the `ddl` folder.
3. Scripts must only insert into/create one table.
4. The names of scripts need to follow the convention: `schema.table.sql`. While it is not required to be lowercase I **HIGHLY RECOMMEND** that you do so. If you do make these names case sensitive, then you will have to follow that casing when referencing the table in the cli.

## An example directory structure.
In your `$HOME` directory create a directory `sql`. In that directory create the directories `ddl` and `insert`. To add a script to populate the table `schema.table_a` we will create the file `schema.table_a.sql` in both the `~/sql/ddl` and `~sql/insert` directories.
```
~/sql/
 -- ddl/
 ---- schema.table_a.sql
 -- sql/
 ---- schema.table_a.sql
```
Now to point `sql-scheduler` to these directories we will set the following environment variables.
```
export SQL_SCHEDULER_DDL_DIRECTORY="~/sql/ddl/"
export SQL_SCHEDULER_INSERT_DIRECTORY="~/sql/insert/"
```
If you are only working with one suite of SQL scripts or one database, you may find it useful to add these export statements to your `.bashrc`/`.zshrc`
```
# .zshrc
echo 'export SQL_SCHEDULER_DDL_DIRECTORY="~/sql/ddl/"' >> ~/.zshrc
echo 'export SQL_SCHEDULER_INSERT_DIRECTORY="~/sql/insert/"' >> ~/.zshrc
echo 'export SQL_SCHEDULER_DSN="postgres://user:password@host:port/database?option=value"' >> ~/.zshrc
source ~/.zshrc
```
```
# .bashrc
echo 'export SQL_SCHEDULER_DDL_DIRECTORY="~/sql/ddl/"' >> ~/.bashrc
echo 'export SQL_SCHEDULER_INSERT_DIRECTORY="~/sql/insert/"' >> ~/.bashrc
echo 'export SQL_SCHEDULER_DSN="postgres://user:password@host:port/database?option=value"' >> ~/.bashrc
source ~/.bashrc
```
