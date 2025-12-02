-- mart_revenue_analysis.sql
-- Business Questions: Q1 (revenue by category/month) + Q2 (payment method analysis)
-- Purpose: Comprehensive revenue analysis by time, category, and payment method
-- Grain: One row per category per month per payment method (most granular level)
-- Source: int_sales_enriched (pre-joined sales data with all dimensions)

{{ config(
    materialized='table',
    schema='marts'
) }}

with base_sales as (
    
    select
        -- Dimensions for grouping
        order_month_date,
        category_name,
        payment_method,
        
        -- Metrics for aggregation
        net_order_amount,
        order_quantity,
        
        -- Edge case flag (Q2 requirement)
        is_zero_quantity_order
        
    from {{ ref('int_sales_enriched') }}
    
    -- Handle edge case: Optionally exclude zero-quantity orders
    -- Keeping them for now so we can flag them in output
    -- where is_zero_quantity_order = false

),
monthly_category_revenue as (
    
    select
        order_month_date,
        category_name,
        
        -- Q1: Total revenue by category and month
        sum(net_order_amount) as monthly_category_revenue,
        
        -- Additional metrics for context
        count(*) as order_count,
        sum(order_quantity) as total_quantity_sold,
        avg(net_order_amount) as avg_order_value,
        
        -- Edge case tracking (Q2)
        sum(case when is_zero_quantity_order then 1 else 0 end) as zero_quantity_order_count
        
    from base_sales
    group by 
        order_month_date,
        category_name

),
payment_method_revenue as (
    
    select
        order_month_date,
        category_name,
        payment_method,
        
        -- Revenue by payment method within each category/month
        sum(net_order_amount) as payment_method_revenue,
        
        -- Order count by payment method
        count(*) as payment_method_order_count
        
    from base_sales
    group by 
        order_month_date,
        category_name,
        payment_method

),
/* **What this does:**
1. **PARTITION BY:** Divides data into groups (month + category)
2. **SUM():** Calculates sum within each partition
3. **OVER:** Keeps all rows (doesn't collapse like GROUP BY)

**Example with real data:**
```
Before window function:
month      | category    | payment_method | revenue
2024-01-01 | Electronics | credit_card    | 1000
2024-01-01 | Electronics | paypal         | 500
2024-01-01 | Electronics | debit_card     | 300
2024-01-01 | Clothing    | credit_card    | 800

After window function adds category_month_total_revenue:
month      | category    | payment_method | revenue | category_month_total
2024-01-01 | Electronics | credit_card    | 1000    | 1800  (1000+500+300)
2024-01-01 | Electronics | paypal         | 500     | 1800  (1000+500+300)
2024-01-01 | Electronics | debit_card     | 300     | 1800  (1000+500+300)
2024-01-01 | Clothing    | credit_card    | 800     | 800   (only payment method)

Now calculate percentage:
2024-01-01 | Electronics | credit_card    | 1000    | 1800 | 55.56%
2024-01-01 | Electronics | paypal         | 500     | 1800 | 27.78%
2024-01-01 | Electronics | debit_card     | 300     | 1800 | 16.67%
2024-01-01 | Clothing    | credit_card    | 800     | 800  | 100.00% */
payment_percentages as (
    
    select
        *,
        
        -- Q2: Calculate percentage of sales by payment method
        -- This requires window functions to calculate totals across partitions
        
        -- Total revenue for this category/month (all payment methods combined)
        sum(payment_method_revenue) over (
            partition by order_month_date, category_name
        ) as category_month_total_revenue,
        
        -- Percentage that this payment method represents, first checking for zero total to avoid division by zero
        case 
            when sum(payment_method_revenue) over (
                partition by order_month_date, category_name
            ) > 0
            then (
                payment_method_revenue / 
                sum(payment_method_revenue) over (
                    partition by order_month_date, category_name
                ) * 100
            )::decimal(5,2)
            else 0
        end as payment_method_pct_of_category_month
        
    from payment_method_revenue

),
final_with_category_totals as (
    
    -- Join payment method details with monthly category totals from Q1
    select
        pp.*,
        mcr.monthly_category_revenue,
        mcr.order_count as category_month_order_count,
        mcr.total_quantity_sold as category_month_quantity_sold,
        mcr.avg_order_value as category_month_avg_order_value,
        mcr.zero_quantity_order_count as category_month_zero_qty_orders
        
    from payment_percentages pp
    inner join monthly_category_revenue mcr
        on pp.order_month_date = mcr.order_month_date
        and pp.category_name = mcr.category_name

)

select * from final_with_category_totals