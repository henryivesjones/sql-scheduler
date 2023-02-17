INSERT INTO
  public.caps (
    SELECT
      a.column_a
    , b.column_b
    , c.column_c
    , d.column_d
    FROM
      public.table_a a
      InnER JoiN public.table_b b ON a.column_a = b.column_a
      JOiN public.table_c c ON b.column_b = c.column_b
      LEFT JOIN public.tAble_d d ON c.column_c = d.column_c
      FUlL OUTeR join public.tABle_e e ON d.column_d = e.column_e
      right JOIN public.tabLE_f f ON e.column_e = f.column_e
  )
;
