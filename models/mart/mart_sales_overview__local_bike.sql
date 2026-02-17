with completed_orders as (
    select
        order_id,
        customer_id,
        order_date,
        required_date,
        shipped_date,
        store_id,
        store_name,
        store_city,
        store_state,
        staff_id,
        staff_name,
        sum(line_revenue) as order_revenue,
        sum(quantity) as total_items,
        avg(discount) as avg_discount,
        count(distinct product_id) as distinct_products,
        min(delivery_days) as delivery_days,
        max(case when is_late then 1 else 0 end) as is_late
    from {{ ref('int_order_lines__local_bike') }}
    where order_status = 'Completed'
    group by 1,2,3,4,5,6,7,8,9,10,11
)

select
    *,
    extract(year from order_date) as order_year,
    extract(month from order_date) as order_month,
    format_date('%Y-%m', order_date) as year_month
from completed_orders