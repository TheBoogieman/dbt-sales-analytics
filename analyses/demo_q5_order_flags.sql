/*******************************************************************************
BUSINESS QUESTION:
Q5: Create a dbt model that flags orders for review based on business rules:

- `discount_applied` > 30%
- `shipping_cost` > 10% of `order_amount`
- Handle null values in both `discount_applied` and `shipping_cost`

MODEL: mart_order_flags
*******************************************************************************/

SELECT 
    order_id,
    customer_name,
    order_amount,
    discount_rate,
    shipping_pct_of_order,
    is_high_discount_order,
    is_high_shipping_order,
    needs_review
FROM main_marts.mart_order_flags
WHERE needs_review = true
ORDER BY order_amount DESC;