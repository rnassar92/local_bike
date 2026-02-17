select
    format_date('%Y-%m', order_date) as year_month,
    extract(year from order_date) as order_year,
    extract(month from order_date) as order_month,
    store_id,
    store_name,
    sum(line_revenue) as revenue,
    sum(quantity) as units_sold,
    count(distinct order_id) as orders_count,
    count(distinct customer_id) as unique_customers,
    avg(discount) as avg_discount
from {{ ref('int_order_lines__local_bike') }}
where order_status = 'Completed'
group by 1,2,3,4,5