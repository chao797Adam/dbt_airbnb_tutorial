{% set nights_booked_threshold = 1 %}
select *
from {{ ref('stg_bookings') }}
where nights_booked > {{ nights_booked_threshold }}
order by nights_booked
