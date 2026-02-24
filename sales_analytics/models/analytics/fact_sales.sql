{{ config(
    materialized='incremental',
    unique_key='id'
) }}

select *
from {{ ref('stg_sales_data') }}

{% if is_incremental() %}

where id > (
    select coalesce(max(id), 0)
    from {{ this }}
)

{% endif %}