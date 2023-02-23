/*test*/
insert into public.multiline_comments /*test
test
*/ (
    select /*test*/ column_a/*test*/
    from public./*test*/table_a
)/*test

*/;
