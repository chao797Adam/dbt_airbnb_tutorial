{{ config(
    severity = 'warn'
) }}

-- 这里的逻辑是：找出所有不符合预期的“错误”数据
-- 如果返回行数 > 0，dbt 就会根据上面的配置发出 WARN
select 
    booking_id,
    listing_id,
    booking_amount
from {{ source('airbnb_source', 'bookings') }}
where cast(booking_amount as float) < 200