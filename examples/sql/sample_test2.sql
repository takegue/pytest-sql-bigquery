with dataset as (
    select 1
    union all select 2
    union all select 3
)
, __check_hoge as (
    select 'test' as label, count(1) as actual, 3 as expected from dataset 
)

select * from dataset
