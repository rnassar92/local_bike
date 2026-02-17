with sales as (
    select
        store_id,
        store_name,
        store_city,
        store_state,
        count(distinct order_id) as total_orders,
        sum(line_revenue) as total_revenue,
        sum(quantity) as total_units_sold,
        count(distinct customer_id) as unique_customers,
        avg(discount) as avg_discount
    from {{ ref('int_order_lines__local_bike') }}
    where order_status = 'Completed'
    group by 1,2,3,4
),

all_orders as (
    select
        store_id,
        count(*) as all_orders_count,
        sum(case when order_status = 'Rejected' then 1 else 0 end) as rejected_orders,
        sum(case when order_status = 'Pending' then 1 else 0 end) as pending_orders
    from {{ ref('stg_orders__local_bike') }}
    group by 1
),

delivery as (
    select
        store_id,
        avg(delivery_days) as avg_delivery_days,
        avg(case when is_late then 1.0 else 0.0 end) as late_delivery_rate
    from {{ ref('int_order_lines__local_bike') }}
    where order_status = 'Completed' and delivery_days is not null
    group by 1
),

stock as (
    select
        store_id,
        sum(stock_quantity) as total_stock_units,
        sum(stock_value) as total_stock_value,
        sum(case when is_out_of_stock then 1 else 0 end) as products_out_of_stock,
        count(*) as total_product_references
    from {{ ref('int_stock_detail__local_bike') }}
    group by 1
),

staff_count as (
    select store_id, count(*) as staff_count
    from {{ ref('stg_staffs__local_bike') }}
    where active = 1
    group by 1
)

select
    s.*,
    round(safe_divide(s.total_revenue, s.total_orders), 2) as avg_order_value,
    round(safe_divide(s.total_revenue, s.unique_customers), 2) as revenue_per_customer,

    ao.all_orders_count,
    ao.rejected_orders,
    round(safe_divide(ao.rejected_orders, ao.all_orders_count), 4) as rejection_rate,
    ao.pending_orders,

    d.avg_delivery_days,
    d.late_delivery_rate,

    st.total_stock_units,
    st.total_stock_value,
    st.products_out_of_stock,
    st.total_product_references,

    sc.staff_count,

from sales s
left join all_orders ao on s.store_id = ao.store_id
left join delivery d on s.store_id = d.store_id
left join stock st on s.store_id = st.store_id
left join staff_count sc on s.store_id = sc.store_id