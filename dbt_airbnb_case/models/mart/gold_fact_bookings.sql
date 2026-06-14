{{ config(
    materialized='view',
    schema='gold'
) }}

select
    b.booking_id,
    b.listing_id,
    -- key, useful for joining with listing table
    l.city,
    l.property_type,
    b.booking_date,
    b.total_amount,
    b.booking_status,
    b.ingested_at as last_sync_time
from {{ ref('silver_bookings') }} b
left join {{ ref('silver_listings') }} l on b.listing_id = l.listing_id
