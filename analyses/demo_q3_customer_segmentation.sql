/*******************************************************************************
BUSINESS QUESTION:
Q3: Create a dbt model that segments customers into tiers based on their total purchase amount:

- "High Value": Total purchases >= $1000
- "Medium Value": Total purchases between $500-$999
- "Low Value": Total purchases < $500

Include customer names and calculate the number of orders per customer.

MODEL: mart_customer_segmentation
*******************************************************************************/

SELECT 
    customer_tier,
    customer_name,
    lifetime_net_amount as total_purchases,
    total_orders
FROM main_marts.mart_customer_segmentation
ORDER BY lifetime_net_amount DESC;