with sales as (
    select
        product_id,
        product_name,
        brand_name,
        category_name,
        model_year,
        product_list_price,
        sum(line_revenue) as total_revenue,
        sum(quantity) as total_units_sold,
        count(distinct order_id) as total_orders,
        avg(discount) as avg_discount,
        round(avg(line_revenue), 2) as avg_line_revenue
    from {{ ref('int_order_lines__local_bike') }}
    where order_status = 'Completed'
    group by 1,2,3,4,5,6
),

stock as (
    select
        product_id,
        sum(stock_quantity) as total_stock,
        sum(stock_value) as total_stock_value,
        sum(case when is_out_of_stock then 1 else 0 end) as stores_out_of_stock
    from {{ ref('int_stock_detail__local_bike') }}
    group by 1
)

select
    s.*,
    coalesce(st.total_stock, 0) as current_stock,
    coalesce(st.total_stock_value, 0) as current_stock_value,
    coalesce(st.stores_out_of_stock, 0) as stores_out_of_stock
from sales s
left join stock st on s.product_id = st.product_id