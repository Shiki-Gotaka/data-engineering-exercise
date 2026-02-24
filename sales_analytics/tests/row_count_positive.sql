with cte as (
    select count(*) as cnt
    from {{ ref('fact_sales') }}
)

select *
from cte
where cnt = 0