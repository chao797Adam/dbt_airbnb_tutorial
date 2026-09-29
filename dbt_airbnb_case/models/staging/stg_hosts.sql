{{ config(
    materialized = 'incremental',
    unique_key = 'host_id'
) }}

with
    source as (
        select
            cast(host_id as bigint) as host_id,
            cast(host_name as string) as host_name,
            cast(host_since as date) as host_since,
            cast(is_superhost as boolean) as is_superhost,
            cast(response_rate as double) as response_rate,
            cast(created_at as timestamp) as created_at,
            cast(ingested_at as timestamp) as ingested_at,
            source_file
        from {{ source('airbnb_source', 'hosts') }}

        {% if is_incremental() %}
            where
                ingested_at
                > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
        {% endif %}
    )
select *
from source
qualify row_number() over (partition by host_id order by ingested_at desc) = 1
