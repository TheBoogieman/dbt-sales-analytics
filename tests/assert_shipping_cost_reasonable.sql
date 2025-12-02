-- tests/assert_shipping_cost_reasonable.sql
-- Test: Shipping cost should not exceed order amount
-- (Unreasonable if shipping > 100% of order value)

select
    order_id,
    order_amount,
    shipping_cost,
    shipping_pct_of_order
from {{ ref('stg_sales_fact') }}
where 
    shipping_cost > order_amount