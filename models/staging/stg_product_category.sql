-- stg_product_category.sql
-- Purpose: Standardize and clean product category master data
-- Source: ../files/product_category.csv
-- Grain: One row per product (product_id is unique)

{{ config(
    materialized='view',
    schema='staging'
) }}

with source as (
    
    select * from {{ read_csv_source('raw_data', 'product_category') }}

),

renamed as (

    select

        -- Primary key
        product_id,

        -- Foreign key / Renaming to match product table
        category_id as product_category_id,
        
        -- Product attributes
        trim(category_name) as category_name, 
        trim(subcategory) as subcategory_name,
        trim(brand) as brand_name,

        --Foreign key to supplier table / Data seems to be denormalized, keeping as is
        supplier_id,
        cost_price::decimal(10,2) as cost_price,
        retail_price::decimal(10,2) as retail_price,
        margin_percent::decimal(5,2) as margin_percent,
        stock_level::integer as stock_level,
        reorder_point::integer as reorder_point,

        -- Calculated: Is product discontinued? / Assumes 'FALSE' or 'TRUE' strings
        discontinued::boolean as is_discontinued,
                
        -- Date fields
        launch_date::date as launch_date,
        last_updated::timestamp as last_updated

    from source

)

select * from renamed