-- tests/assert_valid_discount_rate.sql
-- Test: Discount rate should be between 0 and 1 (0% to 100%)
-- Returns rows with invalid discount rates

select
    order_id,
    discount_rate
from {{ ref('stg_sales_fact') }}
where 
    discount_rate < 0
    or discount_rate > 1