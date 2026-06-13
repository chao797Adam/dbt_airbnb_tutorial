{% set cols = ['booking_id', 'nights_booked', 'booking_amount'] %}

select {% for col in cols %} {{ col }}{% if not loop.last %},{% endif %} {% endfor %}
from {{ ref('stg_bookings') }}
