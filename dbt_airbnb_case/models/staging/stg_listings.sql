{% set incremental_flag = 1 %}
{% set incremental_col = 'ingested_at' %}

{{ config(
    materialized = 'incremental',
    unique_key = 'listing_id'
) }}

SELECT * FROM {{ source('airbnb_source', 'listings') }}

{% if incremental_flag == 1 %}
    WHERE {{ incremental_col }} > (
        SELECT COALESCE(MAX({{ incremental_col }}), '1900-01-01') 
        FROM {{ this }} 
    )
{% endif %}