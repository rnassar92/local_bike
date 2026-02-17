select
    oi.order_id,
    oi.item_id,
    oi.product_id,
    oi.quantity,
    oi.list_price,
    oi.discount,
    oi.line_revenue,
    o.customer_id,
    o.order_status_id,
    o.order_status,
    o.order_date,
    o.required_date,
    o.shipped_date,
    o.store_id,
    o.staff_id,
    date_diff(o.shipped_date, o.order_date, day) as delivery_days,
    case when o.shipped_date > o.required_date then true else false end as is_late,
    p.product_name,
    p.brand_id,
    p.category_id,
    p.model_year,
    p.list_price as product_list_price,
    b.brand_name,
    c.category_name,
    s.store_name,
    s.city as store_city,
    s.state as store_state,
    st.full_name as staff_name

from {{ ref('stg_order_items__local_bike') }} oi
inner join {{ ref('stg_orders__local_bike') }} o on oi.order_id = o.order_id
inner join {{ ref('stg_products__local_bike') }} p on oi.product_id = p.product_id
inner join {{ ref('stg_brands__local_bike') }} b on p.brand_id = b.brand_id
inner join {{ ref('stg_categories__local_bike') }} c on p.category_id = c.category_id
inner join {{ ref('stg_stores__local_bike') }} s on o.store_id = s.store_id
inner join {{ ref('stg_staffs__local_bike') }} st on o.staff_id = st.staff_id