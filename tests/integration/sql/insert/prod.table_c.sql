INSERT INTO
  prod.table_c (
    SELECT
      DISTINCT a.column_a
    , a.column_b
    FROM
      prod.table_a a
  )
;
