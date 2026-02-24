{{ config(materialized='table') }}

select
    sum(total_sales) as total_revenue,
    count(*) as total_transactions,
    round(avg(total_sales)::numeric, 2) as avg_order_value
from {{ ref('fact_sales') }}