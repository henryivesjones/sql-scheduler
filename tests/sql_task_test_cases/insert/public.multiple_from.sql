INSERT INTO
  public.single_from (
    SELECT
      *
    FROM
      public.table_a
    UNION ALL
    SELECT
      *
    FROM
      public.table_b
  )
;
