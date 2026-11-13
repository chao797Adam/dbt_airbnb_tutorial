{{ config(
    severity = 'warn'
) }}

select booking_id, listing_id, booking_amount
from {{ source('airbnb_source', 'bookings') }}
where cast(booking_amount as float) < 200
