{{
  config(
    materialized = 'incremental',
    unique_key = 'booking_id'
  )
}}

select *
from {{ source('airbnb_source', 'bookings') }}

{% if is_incremental() %}
    where
        ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
{% endif %}
