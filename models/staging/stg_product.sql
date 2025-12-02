-- stg_product.sql
-- Purpose: Standardize and clean product master data
-- Source: ../files/product.csv
-- Grain: One row per product (customer_id is unique)

{{ config(
    materialized='view',
    schema='staging'
) }}

with source as (
    
    select * from {{ read_csv_source('raw_data', 'product') }}

),

renamed as (
    
    select
        -- Primary key
        product_id,
        
        -- Customer attributes
        trim(product_name) as product_name,
        
        -- Foreign key
        product_category_id as product_category_id
        
    from source

)

select * from renamed