with
    listings as (select * from {{ ref('silver_listings') }}),
    hosts as (select * from {{ ref('silver_hosts') }}),
    bookings as (select * from {{ ref('silver_bookings') }}),
    obt_test as (
        select
            b.*,
            l.host_id as listing_host_id,
            l.property_type,
            l.room_type,
            l.city,
            l.country,
            l.accommodates,
            l.bathrooms,
            l.bedrooms,
            l.price_per_night,
            l.price_per_night_tag,
            h.host_name,
            h.host_since,
            h.is_superhost,
            h.response_rate,
            h.response_rate_tag
        from bookings b
        left join listings l on b.listing_id = l.listing_id
        left join hosts h on l.host_id = h.host_id
    )

select count(*) as obt_rows
from obt_test
;
