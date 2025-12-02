/*******************************************************************************
BUSINESS QUESTION:
Q7: Create a dbt macro that accepts date range parameters and filters sales data accordingly. 
Use this macro in a model to calculate daily revenue totals. 
Handle cases where no data exists for the given date range.

MODEL: mart_daily_revenue
MACRO: custom_date_filter
*******************************************************************************/

SELECT 
    order_date,
    total_revenue,
    order_count,
    avg_order_value,
    is_no_data_day
FROM main_marts.mart_daily_revenue_january
ORDER BY order_date;

--OR ALTERNATIVELY, if the macro usage is to be shown here, not through the mart model:
SELECT 
*
FROM main_intermediate.int_sales_enriched
WHERE {{ custom_date_filter('','2024-05-01') }};
