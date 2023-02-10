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

# Automatic Inter-Script Dependency Management
Before execution of a run, `sql-scheduler` parses all of the scripts found in the `ddl` and `insert` folders and identifies dependencies between scripts. It is able to do this by identifying tables referenced in `FROM` and `JOIN` statements within the `insert` query. During the execution of a run `sql-scheduler` ensures that any upstream dependencies have completed successfully before executing.

`sql-scheduler` will notify you of any circular dependencies found and exit. This can be checked without initiating a run with the flag `--check`.
