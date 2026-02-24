{{ config(
    materialized='table'
) }}

with base as (
    select *
    from {{ ref('fact_sales') }}
),

aggregated as (
    select
        coalesce(region, 'unknown') as region,
        count(distinct id) as total_transactions,
        coalesce(sum(total_sales), 0) as total_revenue,
        coalesce(avg(total_sales), 0) as avg_sales,
        coalesce(sum(quantity_sold), 0) as total_quantity
    from base
    group by 1

)
select
    region,
    total_transactions,
    round(total_revenue::numeric, 2) as total_revenue,
    round(avg_sales::numeric, 2) as avg_sales,
    total_quantity
from aggregated