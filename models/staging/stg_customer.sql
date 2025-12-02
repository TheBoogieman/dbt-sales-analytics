-- stg_customer.sql
-- Purpose: Standardize and clean customer master data
-- Source: ../files/customer.csv
-- Grain: One row per customer (customer_id is unique)

{{ config(
    materialized='view',
    schema='staging'
) }}

with source as (
    
    select * from {{ read_csv_source('raw_data', 'customer') }}

),

renamed as (
    
    select
        -- Primary key
        customer_id,
        
        -- Customer attributes
        trim(customer_name) as customer_name,
        lower(trim(customer_email)) as customer_email,
        
        -- Date fields - subscription/account lifecycle
        start_date::date as account_start_date,
        end_date::date as account_end_date,
        
        -- Status field - standardize to lowercase
        lower(trim(status)) as account_status,
        
        -- Calculated: Is customer currently active? / Assumes 'active' and 'cancelled' statuses
        case 
            when lower(trim(status)) = 'active' then true
            else false
        end as is_active,
        
        -- Calculated: Account tenure in days
        -- Useful for customer segmentation later
        case 
            when end_date is not null 
            then end_date::date - start_date::date
            else current_date - start_date::date
        end as account_tenure_days,
        
        -- Calculated: Is account expired?
        case 
            when end_date::date < current_date then true
            else false
        end as is_expired

    from source

)

select * from renamed