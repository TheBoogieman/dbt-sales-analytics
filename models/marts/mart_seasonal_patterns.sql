-- mart_seasonal_patterns.sql
-- Business Question: Q6 - Seasonal sales pattern analysis
-- Purpose: Time-series analysis of category performance with growth and volatility metrics
-- Grain: One row per category per month

{{ config(
    materialized='table',
    schema='marts'
) }}

with monthly_sales as (
    
    -- Monthly sales trends by category (Requirement 1)
    select
        order_month_date,
        order_year as year,
        order_month as month,
        order_quarter as quarter,
        category_name,
        sum(net_order_amount) as monthly_revenue
    from {{ ref('int_sales_enriched') }}
    group by 
        order_month_date,
        order_year,
        order_month,
        order_quarter,
        category_name

),

quarterly_aggregates as (
    
    -- Aggregate to quarterly level for QoQ calculations
    select
        year,
        quarter,
        category_name,
        sum(monthly_revenue) as quarterly_revenue
    from monthly_sales
    group by year, quarter, category_name

),

qoq_growth as (
/*
**LAG function:**
- Gets value from previous row
- PARTITION BY category_name = separate sequence per category
- ORDER BY year, quarter = chronological order
- LAG(..., 1) = get value from 1 row back

**Example:**
```
Category: Electronics
Q1 2024: $1000  (previous = null)
Q2 2024: $1200  (previous = $1000)
Q3 2024: $1100  (previous = $1200)

QoQ Growth Q2: ($1200 - $1000) / $1000 * 100 = 20%
QoQ Growth Q3: ($1100 - $1200) / $1200 * 100 = -8.33%
*/
    -- Quarter-over-quarter growth rates (Requirement 2)
    select
        *,
        lag(quarterly_revenue, 1) over (
            partition by category_name 
            order by year, quarter
        ) as previous_quarter_revenue,
        
        case 
            when lag(quarterly_revenue, 1) over (
                partition by category_name 
                order by year, quarter
            ) > 0
            then (
                (quarterly_revenue - lag(quarterly_revenue, 1) over (
                    partition by category_name 
                    order by year, quarter
                )) / lag(quarterly_revenue, 1) over (
                    partition by category_name 
                    order by year, quarter
                ) * 100
            )::decimal(10,2)
            else null
        end as qoq_growth_rate
        
    from quarterly_aggregates

),

monthly_with_qoq as (
    
    -- Join monthly data with quarterly growth rates
    select
        ms.*,
        qoq.quarterly_revenue,
        qoq.qoq_growth_rate
    from monthly_sales ms
    left join qoq_growth qoq
        on ms.year = qoq.year
        and ms.quarter = qoq.quarter
        and ms.category_name = qoq.category_name

),

best_worst_months as (
    
    -- Best and worst performing months (Requirement 3)
    select
        *,
        row_number() over (
            partition by category_name 
            order by monthly_revenue desc
        ) as revenue_rank_desc,
        
        row_number() over (
            partition by category_name 
            order by monthly_revenue asc
        ) as revenue_rank_asc
        
    from monthly_with_qoq

),

coefficient_of_variation as (
/*
stddev(monthly_revenue) / avg(monthly_revenue) as coefficient_of_variation
```

**What it measures:**
- Relative volatility (standardized across different scales)
- Low CV (< 0.3): Stable, predictable sales
- High CV (> 0.5): Volatile, unpredictable sales

**Example:**
```
Category A: Avg = $1000, Stddev = $200, CV = 0.20 (stable)
Category B: Avg = $500, Stddev = $300, CV = 0.60 (volatile)
*/    
    -- Calculate CV for each category (Requirement 4)
    select
        category_name,
        avg(monthly_revenue) as avg_monthly_revenue,
        stddev(monthly_revenue) as stddev_monthly_revenue,
        case 
            when avg(monthly_revenue) > 0 
            then (stddev(monthly_revenue) / avg(monthly_revenue))::decimal(10,4)
            else null 
        end as coefficient_of_variation
    from best_worst_months
    group by category_name

),

final as (
    
    select
        bw.*,
        cv.avg_monthly_revenue,
        cv.stddev_monthly_revenue,
        cv.coefficient_of_variation,
        
        -- Flag best and worst months
        case when bw.revenue_rank_desc = 1 then true else false end as is_best_month,
        case when bw.revenue_rank_asc = 1 then true else false end as is_worst_month
        
    from best_worst_months bw
    left join coefficient_of_variation cv
        on bw.category_name = cv.category_name

)

select * from final
order by category_name, order_month_date