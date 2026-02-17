select
    order_id,
    customer_id,
    order_status as order_status_id,
    case order_status
        when 1 then 'Pending'
        when 2 then 'Processing'
        when 3 then 'Rejected'
        when 4 then 'Completed'
    end as order_status,
    SAFE_CAST(NULLIF(TRIM(CAST(order_date AS STRING)), 'NULL') AS DATE)      as order_date,
    SAFE_CAST(NULLIF(TRIM(CAST(required_date AS STRING)), 'NULL') AS DATE)   as required_date,
    SAFE_CAST(NULLIF(TRIM(CAST(shipped_date AS STRING)), 'NULL') AS DATE)    as shipped_date,
    store_id,
    staff_id
from {{ source('local_bike', 'orders') }}