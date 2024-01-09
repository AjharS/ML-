create procedure test_proc.cicd_test_proc() 
as
begin
create or replace table test_tbl.cicd_test_tbl as select 1 as id,'Hi' as msg;
end
