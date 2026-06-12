{{ config(
    materialized = 'incremental',
    unique_key = 'listing_id'
) }}

select *
from {{ source('airbnb_source', 'listings') }}

{% if is_incremental() %}
    where
        ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
{% endif %}
