{{ config(
    materialized='view'
) }}

with source as (

    select * 
    from {{ source('raw_data', 'sales_data') }}

),

cleaned as (
    select
        id,
        trim(product_name) as product_name,
        lower(trim(category)) as category,
        cast(price as numeric(10,2)) as price,
        cast(quantity_sold as integer) as quantity_sold,
        cast(sale_date as date) as sale_date,
        lower(trim(region)) as region,
        cast(price * quantity_sold as numeric(12,2)) as total_sales,
        current_timestamp as loaded_at
    from source
    where sale_date is not null
      and id is not null
)

select * from cleaned