-- tests/assert_positive_amounts.sql
-- Test: Order amounts should always be positive
-- Returns rows that violate this rule (failing rows)

select
    order_id,
    order_amount,
    net_order_amount
from {{ ref('stg_sales_fact') }}
where 
    order_amount <= 0
    or net_order_amount <= 0