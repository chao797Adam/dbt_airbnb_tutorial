{% snapshot listings_snapshot %}

    {{
    config(
      target_schema='snapshots',
      unique_key='listing_id',
      strategy='check',
      check_cols=['price_per_night', 'room_type', 'bedrooms', 'bathrooms', 'accommodates', 'city', 'country']
    )
}}

    select *
    from {{ ref('stg_listings') }}

{% endsnapshot %}
