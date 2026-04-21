{{ config(materialized='incremental', unique_key='listing_id') }}

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
   -- 关键点：把 SQL 表达式放进引号里作为参数传给宏
   {{ tag_col("cast(price_per_night as int)") }} as price_per_night_tag,
   created_at,
   ingested_at,
   source_file
from {{ ref('stg_listings') }}

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}