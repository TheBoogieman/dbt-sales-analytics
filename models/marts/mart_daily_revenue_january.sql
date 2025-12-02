-- mart_daily_revenue_january.sql
-- Demonstrates macro with parameters

{{ config(
    materialized='table',
    schema='marts'
) }}

with sales_in_range as (
    
    select
        order_date,
        net_order_amount
    from {{ ref('int_sales_enriched') }}
    where {{ custom_date_filter('2024-01-01', '2024-01-31') }}  
    -- January only/default column: order_date
    -- Could also filter by created_at instead of order_date
    -- where {#{{ custom_date_filter('2024-01-01', '2024-01-31', 'order_created_at') }}#}

),

daily_revenue as (
    
    select
        order_date,
        sum(net_order_amount) as total_revenue,
        count(*) as order_count
    from sales_in_range
    group by order_date

)

select * from daily_revenue
order by order_date