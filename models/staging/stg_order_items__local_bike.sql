select
    order_id,
    item_id,
    product_id,
    quantity,
    list_price,
    discount,
    round(quantity * list_price * (1 - discount), 2) as line_revenue
from {{ source('local_bike', 'order_items') }}