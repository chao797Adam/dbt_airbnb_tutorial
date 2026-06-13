{{ config(
    materialized='incremental',
    unique_key='booking_id',  
    incremental_strategy='merge'
) }}

select
    booking_id,
    listing_id,
    booking_date,
    nights_booked,
    booking_amount,
    cleaning_fee,
    service_fee,
    {{ multiply_and_round('nights_booked', 'booking_amount', 2) }} as total_amount,
    booking_status,
    created_at,
    ingested_at,
    source_file
from {{ ref('stg_bookings') }}

{% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
