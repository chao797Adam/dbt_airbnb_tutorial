{% set min_nights = var('min_nights', 1) %}
select *
from {{ ref('stg_bookings') }}
where nights_booked >= {{ min_nights }}
