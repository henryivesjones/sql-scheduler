INSERT INTO
  prod.table_b (
    SELECT
      column_a
    , column_d
    FROM
      raw_d.raw_table_b
  )
;
