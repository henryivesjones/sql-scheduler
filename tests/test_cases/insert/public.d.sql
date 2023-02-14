INSERT INTO
  public.d (
    SELECT
      *
    FROM
      public.b
      INNER JOIN public.out_of_scope_2 b ON a.column_a = b.column_a
  )
;
