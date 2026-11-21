{{ config(
    severity = 'warn'
) }}

-- Business assumption:
-- Booking amounts below 200 are flagged for review,
-- not treated as invalid records.
select booking_id, listing_id, booking_amount
from {{ source('airbnb_source', 'bookings') }}
where try_cast(booking_amount as double) < 200
