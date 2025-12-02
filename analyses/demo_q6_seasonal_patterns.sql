/*******************************************************************************
BUSINESS QUESTION:
Q6: Analyze seasonal sales patterns by creating a model that shows:

- Monthly sales trends by product category
- Quarter-over-quarter growth rates
- Identify the best and worst performing months for each category
- Calculate the coefficient of variation to measure sales volatility

MODEL: mart_seasonal_patterns
*******************************************************************************/

SELECT 
    order_month_date,
    category_name,
    monthly_revenue,
    qoq_growth_rate,
    coefficient_of_variation,
    is_best_month,
    is_worst_month
FROM main_marts.mart_seasonal_patterns
ORDER BY category_name, order_month_date;
