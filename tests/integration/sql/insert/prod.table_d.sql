--sql-scheduler-incremental
DELETE FROM
  prod.table_d
WHERE
  column_dt BETWEEN $1 AND $2
;

INSERT INTO
  prod.table_d (
    SELECT
      column_dt
    , column_a
    FROM
      raw_d.raw_table_d d
    WHERE
      d.column_dt BETWEEN $1 AND $2
  )
;
