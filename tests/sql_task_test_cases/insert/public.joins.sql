INSERT INTO
  public.joins (
    SELECT
      a.column_a
    , b.column_b
    , c.column_c
    , d.column_d
    FROM
      public.table_a a
      INNER JOIN public.table_b b ON a.column_a = b.column_a /*inner join public.table_z*/
      JOIN public.table_c c ON b.column_b = c.column_b /*from public.table_y */
      LEFT JOIN public.table_d d ON c.column_c = d.column_c /*right outer join public.table_x*/
      FULL OUTER JOIN public.table_e e ON d.column_d = e.column_e -- inner join public.x
      RIGHT JOIN public.table_f f ON e.column_e = f.column_e
  )
;
