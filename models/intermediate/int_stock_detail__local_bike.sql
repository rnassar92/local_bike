select
    sk.store_id,
    s.store_name,
    sk.product_id,
    p.product_name,
    p.brand_id,
    b.brand_name,
    p.category_id,
    c.category_name,
    p.model_year,
    p.list_price,
    sk.quantity as stock_quantity,
    round(sk.quantity * p.list_price, 2) as stock_value,
    case when sk.quantity = 0 then true else false end as is_out_of_stock

from {{ ref('stg_stocks__local_bike') }} sk
inner join {{ ref('stg_products__local_bike') }} p on sk.product_id = p.product_id
inner join {{ ref('stg_brands__local_bike') }} b on p.brand_id = b.brand_id
inner join {{ ref('stg_categories__local_bike') }} c on p.category_id = c.category_id
inner join {{ ref('stg_stores__local_bike') }} s on sk.store_id = s.store_id