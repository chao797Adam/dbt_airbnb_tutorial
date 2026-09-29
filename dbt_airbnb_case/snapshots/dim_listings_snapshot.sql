{% snapshot listings_snapshot %}

    {{
    config(
      target_schema='snapshots',
      unique_key='listing_id',
      strategy='check',
      check_cols=['price_per_night', 'price_per_night_tag', 'room_type', 'bedrooms', 'bathrooms', 'accommodates', 'city', 'country']
    )
}}

    select *
    from {{ ref('silver_listings') }}

{% endsnapshot %}
