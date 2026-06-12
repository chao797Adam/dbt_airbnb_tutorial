{{
  config(
    materialized = 'incremental',
    unique_key = 'booking_id'
  )
}}

select
    cast(booking_id as string) as booking_id,
    cast(listing_id as string) as listing_id,
    cast(booking_date as date) as booking_date,
    cast(nights_booked as int) as nights_booked,
    cast(booking_amount as double) as booking_amount,
    cast(cleaning_fee as double) as cleaning_fee,
    cast(service_fee as double) as service_fee,
    cast(booking_status as string) as booking_status,
    cast(created_at as timestamp) as created_at,
    cast(ingested_at as timestamp) as ingested_at,
    source_file
from {{ source('airbnb_source', 'bookings') }}

{% if is_incremental() %}
    where
        ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
{% endif %}
