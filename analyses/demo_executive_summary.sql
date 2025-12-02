/*******************************************************************************
EXECUTIVE SUMMARY: Key metrics across all marts
*******************************************************************************/

WITH metrics AS (
    SELECT 'Total Revenue' as metric, ROUND(SUM(total_revenue), 2) as value FROM main_marts.mart_daily_revenue
    UNION ALL
    SELECT 'Total Customers', COUNT(*) FROM main_marts.mart_customer_segmentation
    UNION ALL
    SELECT 'High Value Customers', COUNT(*) FROM main_marts.mart_customer_segmentation WHERE customer_tier = 'High Value'
    UNION ALL
    SELECT 'Orders Flagged', COUNT(*) FROM main_marts.mart_order_flags WHERE needs_review = true
)
SELECT * FROM metrics;
