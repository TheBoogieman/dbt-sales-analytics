-- mart_daily_revenue.sql
-- Business Question: Q7 - Daily revenue totals with dynamic date filtering
-- Purpose: Calculate daily revenue using parameterized date range macro
-- Grain: One row per day

{{ config(
    materialized='table',
    schema='marts'
) }}

with sales_in_range as (
    
    select
        order_date,
        net_order_amount,
        order_quantity
    from {{ ref('int_sales_enriched') }}
    where {{ custom_date_filter() }}  -- Uses macro with no parameters = all dates

),

daily_revenue as (
    
    select
        order_date,
        sum(net_order_amount) as total_revenue,
        count(*) as order_count,
        sum(order_quantity) as total_units_sold,
        avg(net_order_amount)::decimal(10,2) as avg_order_value
    from sales_in_range
    group by order_date

),

-- Handle case where no data exists for date range (Q7 requirement)
final as (
    
    select
        *,
        case 
            when order_count = 0 then true 
            else false 
        end as is_no_data_day
    from daily_revenue

)

select * from final
order by order_date