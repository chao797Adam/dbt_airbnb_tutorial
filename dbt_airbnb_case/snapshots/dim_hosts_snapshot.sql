{% snapshot hosts_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='host_id',
      strategy='timestamp',
      updated_at='ingested_at',
      dbt_valid_to_current = "to_date('9999-12-31')"
    )
}}

select * from {{ ref('stg_hosts') }}

{% endsnapshot %}