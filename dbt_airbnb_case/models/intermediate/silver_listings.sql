{{ config(materialized='incremental', unique_key='listing_id') }}

with
    source as (
        select
            listing_id,
            host_id,
            property_type,
            room_type,
            city,
            country,
            accommodates,
            bathrooms,
            bedrooms,
            price_per_night,
            {{ tag_col("cast(price_per_night as int)") }} as price_per_night_tag,
            created_at,
            ingested_at,
            source_file
        from {{ ref('stg_listings') }}

        {% if is_incremental() %}
            where ingested_at > (select max(ingested_at) from {{ this }})
        {% endif %}
    )

select *
from source
qualify row_number() over (partition by listing_id order by ingested_at desc) = 1
