{{ config(
    materialized='table'
) }}

with base as (
    select *
    from {{ ref('fact_sales') }}
),
aggregated as (
    select
        coalesce(category, 'unknown') as category,
        count(distinct id) as total_transactions,
        coalesce(sum(total_sales), 0) as total_revenue,
        coalesce(avg(price), 0) as avg_price,
        coalesce(sum(quantity_sold), 0) as total_quantity
    from base
    group by 1
)

select
    category,
    total_transactions,
    round(total_revenue::numeric, 2) as total_revenue,
    round(avg_price::numeric, 2) as avg_price,
    total_quantity
from aggregated