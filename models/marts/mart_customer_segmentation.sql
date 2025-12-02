-- mart_customer_segmentation.sql
-- Business Question: Who are our most valuable customers? (Q3)
-- Purpose: Segment customers into value tiers for targeted marketing and retention
-- Grain: One row per customer
-- Source: int_customer_aggregates (pre-calculated lifetime metrics)

{{ config(
    materialized='table',
    schema='marts'
) }}

with customer_metrics as (
    
    -- Pull pre-calculated customer lifetime metrics
    select * from {{ ref('int_customer_aggregates') }}

),

customer_tiers as (

    select
        -- ============================================
        -- CUSTOMER IDENTIFIERS
        -- ============================================
        customer_id,
        customer_name,
        customer_email,
        
        -- ============================================
        -- PURCHASE BEHAVIOR METRICS
        -- ============================================
        
        -- Total amount customer has spent (after discounts)
        lifetime_net_amount,
        
        -- Number of orders placed
        total_orders,
        
        -- Average amount per order
        avg_order_value,
        
        -- First and last order dates (customer activity span)
        first_order_date,
        last_order_date,
        customer_lifespan_days,
        
        -- ============================================
        -- CUSTOMER SEGMENTATION (Q3 Business Logic)
        -- ============================================
        
        -- Primary tier classification based on lifetime spend
        case 
            when lifetime_net_amount >= 1000 then 'High Value'
            when lifetime_net_amount >= 500 then 'Medium Value'
            else 'Low Value'
        end as customer_tier,
        
        -- ============================================
        -- ADDITIONAL SEGMENTATION DIMENSIONS
        -- ============================================
        
        -- Order frequency segmentation
        case 
            when total_orders >= 10 then 'Frequent Buyer'
            when total_orders >= 5 then 'Regular Buyer'
            when total_orders >= 2 then 'Repeat Buyer'
            else 'One-Time Buyer'
        end as order_frequency_segment,
        
        -- Recency segmentation (days since last order)
        case 
            when last_order_date >= current_date - interval '30 days' then 'Active'
            when last_order_date >= current_date - interval '90 days' then 'Lapsing'
            when last_order_date >= current_date - interval '180 days' then 'At Risk'
            else 'Inactive'
        end as recency_segment,
        
        -- Average order value segmentation
        case 
            when avg_order_value >= 200 then 'High AOV'
            when avg_order_value >= 100 then 'Medium AOV'
            else 'Low AOV'
        end as aov_segment,
        
        -- ============================================
        -- CALCULATED METRICS FOR ANALYSIS
        -- ============================================
        
        -- Days since last order (recency in days)
        current_date - last_order_date as days_since_last_order,
        
        -- Average days between orders (frequency metric)
        case 
            when total_orders > 1 
            then customer_lifespan_days::decimal / (total_orders - 1)
            else null 
        end as avg_days_between_orders,
        
        -- Percentage of discount usage
        pct_orders_with_discount,
        
        -- Payment and product preferences
        preferred_payment_method,
        favorite_category,
        unique_products_purchased,
        unique_categories_purchased,
        
        -- Account status
        account_status,
        is_active_customer

    from customer_metrics

),

-- Add ranking within each tier for prioritization
tier_rankings as (

    select
        *,
        
        -- Rank customers within their tier by lifetime value
        row_number() over (
            partition by customer_tier 
            order by lifetime_net_amount desc
        ) as rank_within_tier,
        
        -- Overall customer value rank
        row_number() over (
            order by lifetime_net_amount desc
        ) as overall_value_rank

    from customer_tiers

)

select * from tier_rankings