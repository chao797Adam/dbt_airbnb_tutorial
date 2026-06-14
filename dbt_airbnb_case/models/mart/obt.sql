{{ config(materialized='table', schema='gold') }}

with
    bookings as (select * from {{ ref('silver_bookings') }}),
    listings as (select * from {{ ref('silver_listings') }}),
    hosts as (select * from {{ ref('silver_hosts') }})

select
    -- === Bookings table columns ===
    b.booking_id,
    b.listing_id,
    b.booking_date,
    b.nights_booked,
    b.booking_amount,
    b.cleaning_fee,
    b.service_fee,
    b.booking_amount_rounded,
    b.total_booking_amount,
    b.booking_status,
    b.created_at as booking_created_at,
    b.ingested_at as booking_ingested_at,
    b.source_file as booking_source_file,

    -- === Listings table columns ===
    l.property_type,
    l.room_type,
    l.city,
    l.country,
    l.accommodates,
    l.bathrooms,
    l.bedrooms,
    l.price_per_night,
    l.price_per_night_tag,
    l.created_at as listing_created_at,
    l.ingested_at as listing_ingested_at,

    -- === Hosts table columns ===
    h.host_id,
    h.host_name,
    h.host_since,
    h.is_superhost,
    h.response_rate,
    h.response_rate_tag,
    h.created_at as host_created_at,
    h.ingested_at as host_ingested_at

from bookings b
left join listings l on b.listing_id = l.listing_id
left join hosts h on l.host_id = h.host_id
