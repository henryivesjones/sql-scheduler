INSERT INTO
  public.a (
    SELECT
      *
    FROM
      public.out_of_scope a
      INNER JOIN public.out_of_scope_2 b ON a.column_a = b.column_a
  )
;
