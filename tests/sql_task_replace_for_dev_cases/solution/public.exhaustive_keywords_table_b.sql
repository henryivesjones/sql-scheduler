DROP TABLE
  if exists dev_schema.table_a
;

DROP TABLE
  dev_schema.table_a
;

CREATE TABLE
  if NOT exists dev_schema.table_a (column_a INT, column_b INT, column_c VARCHAR)
;

CREATE TABLE
  dev_schema.table_a (column_a INT, column_b INT, column_c VARCHAR)
;

UPDATE
  dev_schema.table_a
SET
  column_a = 1
;

UPDATE
  dev_schema.table_a
SET
  column_a = 1
WHERE
  column_b = 5
;

INSERT INTO
  dev_schema.table_a
VALUES
  (1, 2, 3)
;

INSERT INTO
  dev_schema.table_a (column_a)
VALUES
  (1)
;

INSERT INTO
  dev_schema.table_a (
    SELECT
      *
    FROM
      dev_schema.table_b b
      INNER JOIN public.table_c c ON b.column_b = c.column_b
  )
;

INSERT INTO
  dev_schema.table_a (column_b, column_c) (
    SELECT
      b.column_b
    , c.column_c
    FROM
      dev_schema.table_b b
      LEFT JOIN public.table_c c ON b.column_b = c.column_b
  )
;

DELETE FROM
  dev_schema.table_a
WHERE
  column_a = 1
;

DELETE FROM
  dev_schema.table_b
WHERE
  column_b = 2
;
