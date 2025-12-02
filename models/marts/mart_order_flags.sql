-- mart_order_flags.sql
-- Business Question: Which orders need review due to unusual discounts or shipping costs? (Q5)
-- Purpose: Identify potentially problematic orders for operations team review
-- Grain: One row per order (can be filtered to show only flagged orders)
-- Source: int_sales_enriched (pre-calculated flags and metrics)

{{ config(
    materialized='table',
    schema='marts'
) }}

with sales_enriched as (
    
    -- Pull enriched sales data with pre-calculated flags
    select * from {{ ref('int_sales_enriched') }}

),

order_review_details as (

    select
        -- ============================================
        -- ORDER IDENTIFIERS
        -- ============================================
        order_id,
        order_date,
        order_created_at,
        
        -- ============================================
        -- CUSTOMER CONTEXT
        -- ============================================
        customer_id,
        customer_name,
        customer_email,
        
        -- ============================================
        -- ORDER FINANCIAL DETAILS
        -- ============================================
        order_amount,
        discount_rate,
        shipping_cost,
        net_order_amount,
        order_quantity,
        
        -- Calculated: Discount amount in dollars
        (order_amount - net_order_amount) as discount_amount_dollars,
        
        -- Shipping as percentage (already calculated in intermediate)
        shipping_pct_of_order,
        
        -- ============================================
        -- PRODUCT CONTEXT
        -- ============================================
        product_id,
        product_name,
        category_name,
        
        -- ============================================
        -- REVIEW FLAGS (Q5 Business Rules)
        -- ============================================
        
        -- Flag: Discount exceeds 30% threshold
        is_high_discount_order,
        
        -- Flag: Shipping exceeds 10% of order amount threshold
        is_high_shipping_order,
        
        -- Flag: Order needs review (either condition met)
        needs_review,
        
        -- ============================================
        -- NULL HANDLING TRANSPARENCY (Q5 Requirement)
        -- ============================================
        
        -- Show if discount was null in source data
        case 
            when discount_rate is null then true 
            else false 
        end as had_null_discount,
        
        -- Show if shipping was null in source data
        case 
            when shipping_cost is null then true 
            else false 
        end as had_null_shipping,
        
        -- ============================================
        -- REVIEW PRIORITIZATION
        -- ============================================
        
        -- Severity scoring: how many rules violated?
        (case when is_high_discount_order then 1 else 0 end +
         case when is_high_shipping_order then 1 else 0 end) as violation_count,
        
        -- Severity classification
        case
            when is_high_discount_order and is_high_shipping_order then 'Critical'
            when is_high_discount_order or is_high_shipping_order then 'Warning'
            else 'Normal'
        end as severity_level,
        
        -- Dollar amount at risk (discount + excess shipping)
        (discount_amount_dollars + 
         case 
            when shipping_pct_of_order > 0.10 
            then (shipping_cost - (order_amount * 0.10))
            else 0 
         end)::decimal(10,2) as at_risk_amount

    from sales_enriched

),

-- Add ranking for prioritization
prioritized_orders as (

    select
        *,
        
        -- Rank flagged orders by risk amount
        case 
            when needs_review 
            then row_number() over (
                partition by needs_review 
                order by at_risk_amount desc
            )
            else null 
        end as review_priority_rank

    from order_review_details

)

select * from prioritized_orders

-- Optional: Uncomment to show only flagged orders
-- where needs_review = true