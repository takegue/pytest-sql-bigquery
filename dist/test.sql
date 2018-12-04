with 
  test as (
    select 1
  )
  , __check as (
    select "hoge" as label, 1 as actual, 1 as expected
    union all select "fuga" as label, 1 as actual, 1 as expected
  )

select * from test
