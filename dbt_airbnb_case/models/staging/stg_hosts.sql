{{ config(
    materialized = 'incremental',
    unique_key = 'host_id'
) }}

{% set incremental_col = 'ingested_at' %}

select *
from {{ source('airbnb_source', 'hosts') }}

{% if is_incremental() %}
    where
        {{ incremental_col }}
        > (select coalesce(max({{ incremental_col }}), '1900-01-01') from {{ this }})
{% endif %}
