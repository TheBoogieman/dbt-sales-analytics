/*******************************************************************************
BUSINESS QUESTION:
Q4: Create a model that analyzes payment method preferences by calculating:

- Total revenue by payment method
- Average order value by payment method
- Number of orders by payment method
- Percentage distribution of each payment method

MODEL: mart_payment_method_analysis
*******************************************************************************/

SELECT 
    payment_method,
    total_revenue,
    avg_order_value,
    order_count,
    pct_of_total_revenue
FROM main_marts.mart_payment_method_analysis
ORDER BY total_revenue DESC;