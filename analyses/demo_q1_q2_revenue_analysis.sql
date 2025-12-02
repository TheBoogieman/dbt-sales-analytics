/*******************************************************************************
BUSINESS QUESTIONS:
Q1: Create a dbt model that calculates total revenue by product category for each month. 
Include basic data transformations and aggregations.

Q2: Extend the previous model to handle edge cases where `order_quantity` is zero and calculate the percentage of sales coming from each payment method. 
Handle null values appropriately.

MODEL: mart_revenue_analysis
*******************************************************************************/

SELECT 
    order_month_date,
    category_name,
    payment_method,
    monthly_category_revenue as category_total,
    payment_method_revenue,
    payment_method_pct_of_category_month as payment_method_pct,
    category_month_zero_qty_orders as zero_qty_orders
FROM main_marts.mart_revenue_analysis
ORDER BY category_name, order_month_date  DESC;