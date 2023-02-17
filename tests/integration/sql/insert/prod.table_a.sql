INSERT INTO
  prod.table_a (
    SELECT
      column_a
    , column_b
    , column_c
    FROM
      raw_d.raw_table_a
  )
;
