-- int_sales_enriched.sql
-- Purpose: Denormalized order fact table with complete dimensional context
-- Grain: One row per order_id
-- Usage: Foundation for all sales analysis (Q1, Q2, Q4, Q5, Q6, Q7)
-- Materialization: Table (expensive 4-way join, queried by multiple marts)

{{ config(
    materialized='table',
    schema='intermediate'
) }}

-- Step 1: Reference all staging models in separate CTEs for clarity
with sales as (
    select * from {{ ref('stg_sales_fact') }}
),

customers as (
    select * from {{ ref('stg_customer') }}
),

products as (
    select * from {{ ref('stg_product') }}
),

product_categories as (
    select * from {{ ref('stg_product_category') }}
),

-- Step 2: Perform joins and add calculated business metrics
sales_enriched as (
    select
        -- ============================================
        -- ORDER IDENTIFIERS & DATES
        -- ============================================
        sales.order_id,
        sales.order_date,
        sales.order_created_at,
        
        -- Extract date parts for time-based analysis (Q1, Q6)
        extract(year from sales.order_date) as order_year,
        extract(month from sales.order_date) as order_month,
        extract(quarter from sales.order_date) as order_quarter,
        date_trunc('month', sales.order_date) as order_month_date,
        
        -- ============================================
        -- SALES FACT METRICS
        -- ============================================
        sales.order_amount,
        sales.order_quantity,
        sales.discount_rate,
        sales.shipping_cost,
        sales.net_order_amount,
        sales.payment_method,
        
        -- Flags from staging
        sales.is_discounted,
        sales.shipping_pct_of_order,
        
        -- ============================================
        -- CUSTOMER DIMENSION
        -- ============================================
        sales.customer_id,
        customers.customer_name,
        customers.customer_email,
        customers.account_status,
        customers.is_active as is_active_customer,
        customers.account_tenure_days as customer_tenure_days,
        
        -- ============================================
        -- PRODUCT DIMENSION
        -- ============================================
        sales.product_id,
        products.product_name,
        products.product_category_id,
        
        -- Category details from denormalized table
        product_categories.category_name,
        product_categories.subcategory_name,
        product_categories.brand_name,
        
        -- ============================================
        -- FINANCIAL METRICS (Cost & Pricing)
        -- ============================================
        product_categories.cost_price as unit_cost_price,
        product_categories.retail_price as unit_retail_price,
        product_categories.margin_percent as product_margin_pct,
        
        -- ============================================
        -- CALCULATED BUSINESS METRICS
        -- ============================================
        
        -- Total cost of goods sold (COGS) for this order
        (sales.order_quantity * product_categories.cost_price)::decimal(10,2) 
            as total_cogs,
        
        -- Gross profit: Net revenue minus cost of goods
        (sales.net_order_amount - (sales.order_quantity * product_categories.cost_price))::decimal(10,2) 
            as gross_profit,
        
        -- Gross margin percentage
        case 
            when sales.net_order_amount > 0 
            then (
                (sales.net_order_amount - (sales.order_quantity * product_categories.cost_price)) 
                / sales.net_order_amount * 100
            )::decimal(5,2)
            else 0 
        end as gross_margin_pct,
        
        -- Net revenue after shipping costs
        (sales.net_order_amount - sales.shipping_cost)::decimal(10,2) 
            as net_revenue_after_shipping,
        
        -- ============================================
        -- EDGE CASE HANDLING (Q2, Q5)
        -- ============================================
        
        -- Flag: Zero quantity orders (edge case from Q2)
        case 
            when sales.order_quantity = 0 then true 
            else false 
        end as is_zero_quantity_order,
        
        -- Flag: High discount orders (Q5 business rule: >30%)
        case 
            when sales.discount_rate > 0.30 then true 
            else false 
        end as is_high_discount_order,
        
        -- Flag: High shipping orders (Q5 business rule: >10% of order amount)
        case 
            when sales.shipping_pct_of_order > 0.10 then true 
            else false 
        end as is_high_shipping_order,
        
        -- Combined flag: Needs review (either high discount OR high shipping)
        case 
            when sales.discount_rate > 0.30 
                or sales.shipping_pct_of_order > 0.10 
            then true 
            else false 
        end as needs_review

    from sales
    
    -- Join customer (INNER: every order must have a customer)
    inner join customers 
        on sales.customer_id = customers.customer_id
    
    -- Join product (INNER: every order must have a product)
    inner join products 
        on sales.product_id = products.product_id
    
    -- Join category details (INNER: every product must have category info)
    inner join product_categories 
        on sales.product_id = product_categories.product_id
)

select * from sales_enriched