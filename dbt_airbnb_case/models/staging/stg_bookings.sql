{% set incremental_flag = 1 %}
{% set incremental_col = 'ingested_at' %}

SELECT * FROM {{ source('airbnb_source', 'bookings') }}

{% if incremental_flag == 1 %}
    WHERE {{ incremental_col }} > (
        SELECT COALESCE(MAX({{ incremental_col }}), '1900-01-01') 
        FROM {{ this }} 
    )
{% endif %}