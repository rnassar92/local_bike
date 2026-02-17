select
    staff_id,
    first_name,
    last_name,
    concat(first_name, ' ', last_name) as full_name,
    email,
    phone,
    active,
    store_id,
    cast(manager_id as int64) as manager_id
from {{ source('local_bike', 'staffs') }}