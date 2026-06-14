{% snapshot hosts_snapshot %}

    {{
    config(
      target_schema='snapshots',
      unique_key='host_id',
      strategy='check',
      check_cols=[
          'host_name',
          'is_superhost',
          'response_rate'
      ]
    )
}}

    select *
    from {{ ref('stg_hosts') }}

{% endsnapshot %}
