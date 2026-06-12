{{ config(
    materialized = 'incremental',
    unique_key = 'listing_id'
) }}

select
    cast(listing_id as string) as listing_id,
    cast(host_id as string) as host_id,
    cast(property_type as string) as property_type,
    cast(room_type as string) as room_type,
    cast(city as string) as city,
    cast(country as string) as country,
    cast(accommodates as int) as accommodates,
    cast(bathrooms as double) as bathrooms,
    cast(bedrooms as double) as bedrooms,
    cast(price_per_night as double) as price_per_night,
    cast(created_at as timestamp) as created_at,
    cast(ingested_at as timestamp) as ingested_at,
    source_file
from {{ source('airbnb_source', 'listings') }}

{% if is_incremental() %}
    where
        ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
{% endif %}
