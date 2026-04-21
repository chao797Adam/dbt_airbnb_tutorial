{{ config(materialized='table', schema='gold') }}

WITH bookings AS ( SELECT * FROM {{ ref('silver_bookings') }} ),
     listings AS ( SELECT * FROM {{ ref('silver_listings') }} ),
     hosts    AS ( SELECT * FROM {{ ref('silver_hosts') }} )

SELECT
    -- === Bookings 表所有列 ===
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
    b.created_at AS booking_created_at,
    b.ingested_at AS booking_ingested_at,
    b.source_file AS booking_source_file,

    -- === Listings 表所有列 ===
    l.property_type,
    l.room_type,
    l.city,
    l.country,
    l.accommodates,
    l.bathrooms,
    l.bedrooms,
    l.price_per_night,
    l.price_per_night_tag,
    l.created_at AS listing_created_at,
    l.ingested_at AS listing_ingested_at,

    -- === Hosts 表所有列 ===
    h.host_id,
    h.host_name,
    h.host_since,
    h.is_superhost,
    h.response_rate,
    h.response_rate_tag,
    h.created_at AS host_created_at,
    h.ingested_at AS host_ingested_at

FROM bookings b
LEFT JOIN listings l ON b.listing_id = l.listing_id
LEFT JOIN hosts h ON l.host_id = h.host_id