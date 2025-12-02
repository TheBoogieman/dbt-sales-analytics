-- mart_payment_method_analysis.sql
-- Business Question: Q4 - Payment method preference analysis
-- Purpose: Comprehensive payment method performance metrics for strategic decisions
-- Grain: One row per payment method (aggregate across all time and categories)
-- Source: int_sales_enriched (pre-joined sales with all context)

{{ config(
    materialized='table',
    schema='marts'
) }}

with all_sales as (
    
    -- Get all sales transactions with payment method info
    select
        payment_method,
        net_order_amount,
        order_quantity,
        order_id
    from {{ ref('int_sales_enriched') }}

),
/* percentile_cont(0.25) within group (order by net_order_amount) as p25_order_value,
percentile_cont(0.50) within group (order by net_order_amount) as median_order_value,
percentile_cont(0.75) within group (order by net_order_amount) as p75_order_value,
```

**What percentiles show:**
- **P25 (25th percentile):** 25% of orders are below this value
- **P50 (median):** Middle value (better than average for skewed data)
- **P75 (75th percentile):** 75% of orders are below this value

**Example interpretation:**
```
Credit Card:
- P25: $20 (25% of orders under $20)
- Median: $50 (half of orders under $50)
- P75: $150 (75% of orders under $150)
- Average: $85 (influenced by a few very large orders) */
payment_method_metrics as (
    
    select
        payment_method,
        
        -- Q4 Metric 1: Total revenue by payment method
        sum(net_order_amount) as total_revenue,
        
        -- Q4 Metric 2: Number of orders by payment method
        count(*) as order_count,
        
        -- Q4 Metric 3: Average order value by payment method
        avg(net_order_amount)::decimal(10,2) as avg_order_value,
        
        -- Additional context metrics
        sum(order_quantity) as total_units_sold,
        min(net_order_amount) as min_order_value,
        max(net_order_amount) as max_order_value,
        
        -- Statistical measures for deeper analysis
        stddev(net_order_amount)::decimal(10,2) as stddev_order_value,
        
        -- Percentile analysis (helps understand distribution)
        percentile_cont(0.25) within group (order by net_order_amount)::decimal(10,2) as p25_order_value,
        percentile_cont(0.50) within group (order by net_order_amount)::decimal(10,2) as median_order_value,
        percentile_cont(0.75) within group (order by net_order_amount)::decimal(10,2) as p75_order_value
        
    from all_sales
    group by payment_method

),

payment_percentages as (
/*
1. sum(total_revenue) over () - Grand total revenue across ALL payment methods
No PARTITION BY = one value for entire dataset
Example: $10,000 total across all payment methods

2.total_revenue / sum(...) - This payment method's share
Example: Credit card revenue $5,500 / $10,000 = 0.55

3. * 100 - Convert to percentage
0.55 * 100 = 55%

4. ::decimal(5,2) - Format as XX.XX%
55.00%  

*/
    select
        *,
        
        -- Q4 Metric 4: Percentage distribution of each payment method
        -- Calculate what % of total revenue comes from this payment method
        (total_revenue / sum(total_revenue) over () * 100)::decimal(5,2) 
            as pct_of_total_revenue,
        
        -- Additional percentage metrics
        (order_count::decimal / sum(order_count) over () * 100)::decimal(5,2) 
            as pct_of_total_orders,
        
        (total_units_sold::decimal / sum(total_units_sold) over () * 100)::decimal(5,2) 
            as pct_of_total_units,
        
        -- Revenue per order compared to overall average
        avg_order_value / avg(avg_order_value) over () as aov_index,
        
        -- Rank payment methods by revenue
        row_number() over (order by total_revenue desc) as revenue_rank
        
    from payment_method_metrics

)

select * from payment_percentages
order by revenue_rank