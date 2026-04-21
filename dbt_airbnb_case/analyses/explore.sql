WITH listings AS (
    SELECT * FROM {{ ref('silver_listings') }} -- 包含你刚才写的 tag_col 逻辑
),
hosts AS (
    SELECT * FROM {{ ref('silver_hosts') }}    -- 包含你刚才写的 response_rate_tag 逻辑
),
bookings AS (
    SELECT * FROM {{ ref('silver_bookings') }} -- 包含你刚才写的 total_booking_amount 逻辑
),
obt_test AS (
    SELECT 
        b.*,  -- 包含 Bookings 所有列
        l.host_id AS listing_host_id, -- 避免和 hosts 表的 host_id 冲突
        l.property_type, l.room_type, l.city, l.country, l.accommodates, l.bathrooms, l.bedrooms, l.price_per_night, l.price_per_night_tag,
        h.host_name, h.host_since, h.is_superhost, h.response_rate, h.response_rate_tag
    FROM bookings b
    LEFT JOIN listings l ON b.listing_id = l.listing_id
    LEFT JOIN hosts h ON l.host_id = h.host_id
)

-- 这里的输出会告诉你大宽表的行数
SELECT COUNT(*) as obt_rows FROM obt_test;