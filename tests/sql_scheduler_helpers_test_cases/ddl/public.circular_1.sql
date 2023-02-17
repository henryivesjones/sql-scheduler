DROP TABLE
  if exists public.circular_1
;

CREATE TABLE
  public.circular_1 (
    column_a INT
  , column_b INT
  , column_c INT
  , column_c INT
  , PRIMARY KEY (column_a)
  )
;
