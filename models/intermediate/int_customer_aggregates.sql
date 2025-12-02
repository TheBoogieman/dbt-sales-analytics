-- int_customer_aggregates.sql
-- Purpose: Customer lifetime value and purchase behavior metrics
-- Grain: One row per customer_id
-- Usage: Customer segmentation and analysis (Q3)
-- Materialization: Table (aggregating all orders per customer)

{{ config(
    materialized='table',
    schema='intermediate'
) }}

with enriched_sales as (
    -- Reference the enriched sales table we just created
    select * from {{ ref('int_sales_enriched') }}
),

customer_aggregates as (
    select
        -- ============================================
        -- CUSTOMER IDENTIFIERS
        -- ============================================
        customer_id,
        
        -- Take first non-null value for customer attributes
        -- (should all be same, but max() handles any edge cases)
        max(customer_name) as customer_name,
        max(customer_email) as customer_email,
        max(account_status) as account_status,
        max(is_active_customer) as is_active_customer,
        
        -- ============================================
        -- LIFETIME VALUE METRICS
        -- ============================================
        
        -- Total amount spent (before discounts)
        sum(order_amount) as lifetime_order_amount,
        
        -- Total amount spent (after discounts, before shipping)
        sum(net_order_amount) as lifetime_net_amount,
        
        -- Total spent including shipping
        sum(net_order_amount + shipping_cost) as lifetime_total_spent,
        
        -- Total discounts received
        sum(order_amount - net_order_amount) as lifetime_discount_received,
        
        -- ============================================
        -- ORDER BEHAVIOR METRICS
        -- ============================================
        
        -- Number of orders placed
        count(*) as total_orders,
        
        -- Number of items purchased
        sum(order_quantity) as total_items_purchased,
        
        -- Average order value
        avg(net_order_amount)::decimal(10,2) as avg_order_value,
        
        -- ============================================
        -- TEMPORAL METRICS
        -- ============================================
        
        -- First and last order dates
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        
        -- Days between first and last order (customer lifespan)
        max(order_date) - min(order_date) as customer_lifespan_days,
        
        -- ============================================
        -- PAYMENT & DISCOUNT BEHAVIOR
        -- ============================================
        
        -- Most frequently used payment method
        mode() within group (order by payment_method) as preferred_payment_method,
        
        -- Percentage of orders with discounts
        (sum(case when is_discounted then 1 else 0 end)::decimal / count(*) * 100)::decimal(5,2) 
            as pct_orders_with_discount,
        
        -- ============================================
        -- PRODUCT PREFERENCES
        -- ============================================
        
        -- Number of unique products purchased
        count(distinct product_id) as unique_products_purchased,
        
        -- Number of unique categories purchased from
        count(distinct category_name) as unique_categories_purchased,
        
        -- Most purchased category
        mode() within group (order by category_name) as favorite_category

    from enriched_sales
    group by customer_id
)

select * from customer_aggregates