{{ config(materialized='incremental', unique_key='host_id') }}

select 
    host_id,
    replace(host_name, ' ', '_') as host_name,
    host_since,
    is_superhost,
    response_rate,
    case when response_rate > 95 then 'very_good'
         when response_rate > 80 then 'good'
         when response_rate > 60 then 'fair'
         else 'poor'
         end as response_rate_tag,
    created_at,
    ingested_at,
    source_file
from {{ ref('stg_hosts') }}

{% if is_incremental() %}
  -- 关键优化：只读取比当前 Silver 表中最新记录还要新的数据
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}