DROP TABLE
  if exists public.table_a
;

DROP TABLE
  public.table_a
;

CREATE TABLE
  if NOT exists public.table_a (column_a INT, column_b INT, column_c VARCHAR)
;

CREATE TABLE
  public.table_a (column_a INT, column_b INT, column_c VARCHAR)
;

UPDATE
  public.table_a
SET
  column_a = 1
;

UPDATE
  public.table_a
SET
  column_a = 1
WHERE
  column_b = 5
;

INSERT INTO
  public.table_a
VALUES
  (1, 2, 3)
;

INSERT INTO
  public.table_a (column_a)
VALUES
  (1)
;

INSERT INTO
  public.table_a (
    SELECT
      *
    FROM
      public.table_b b
      INNER JOIN public.table_c c ON b.column_b = c.column_b
  )
;

INSERT INTO
  public.table_a (column_b, column_c) (
    SELECT
      b.column_b
    , c.column_c
    FROM
      public.table_b b
      LEFT JOIN public.table_c c ON b.column_b = c.column_b
  )
;

DELETE FROM
  public.table_a
WHERE
  column_a = 1
;

DELETE FROM
  public.table_b
WHERE
  column_b = 2
;
