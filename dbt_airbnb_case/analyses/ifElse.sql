{% set flag = 2 %}
select *
from {{ ref('stg_bookings') }}
{% if flag == 1 %} where nights_booked > 1
{% else %} where nights_booked = 1
{% endif %}
order by nights_booked
