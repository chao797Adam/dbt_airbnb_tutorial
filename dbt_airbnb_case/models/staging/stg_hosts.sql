{% set incremental_flag = 1 %}
{% set incremental_col = 'created_at' %}

{{ config(
    materialized = 'incremental',
    unique_key = 'host_id'
) }}

SELECT * FROM {{ source('airbnb_source', 'hosts') }}

{% if incremental_flag == 1 %}
    WHERE {{ incremental_col }} > (
        SELECT COALESCE(MAX({{ incremental_col }}), '1900-01-01') 
        FROM {{ this }} 
    )
{% endif %}