-- stg_sales_fact.sql
-- Purpose: Standardize and clean raw sales transaction data
-- Source: ../files/sales_fact.csv
-- Grain: One row per order (order_id is unique)
-- Materialized as: view (lightweight, no data storage)
-- Schema: staging (separates from raw and marts)

{{ config(
    materialized='view',
    schema='staging'
) }}

with source as (
    
    -- Use custom macro to read CSV directly with DuckDB
    -- This compiles to: read_csv_auto('../files/sales_fact.csv', ...)
    select * from {{ read_csv_source('raw_data', 'sales_fact') }}

),

renamed as (
    
    -- Standardize column names, cast types, add calculated fields
    select
        -- Primary key
        order_id,
        
        -- Foreign keys
        product_id,
        customer_id,
        
        -- Date columns: cast strings to proper DATE type
        order_date::date as order_date,
        
        -- Timestamp: cast and rename for clarity
        created_at::timestamp as order_created_at,
        
        -- Financial columns: cast to DECIMAL for precision, handle nulls
        order_amount::decimal(10,2) as order_amount,
        
        -- Quantity: cast to INTEGER
        order_quantity::integer as order_quantity,
        
        -- Text standardization: lowercase and trim whitespace
        lower(trim(payment_method)) as payment_method,
        
        -- Discount: cast and rename for clarity (0.10 = 10% rate) with nulls handled
        coalesce(discount_applied, 0)::decimal(5,4) as discount_rate,

        -- Shipping cost with nulls handled
        coalesce(shipping_cost, 0)::decimal(10,2) as shipping_cost,
        
        -- Calculated: revenue after discount
        (order_amount * (1 - coalesce(discount_applied, 0)))::decimal(10,2) as net_order_amount,
        
        -- Calculated: boolean flag for discounted orders
        case 
            when discount_applied > 0 then true 
            else false 
        end as is_discounted,
        
        -- Calculated: shipping as percentage of order amount handling null by casting to decimal
        case 
            when order_amount > 0 
            then (coalesce(shipping_cost, 0) / order_amount)::decimal(5,4)
            else 0 
        end as shipping_pct_of_order

    from source

)

-- Final select
select * from renamed