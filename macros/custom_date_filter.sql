{% macro custom_date_filter(start_date=none, end_date=none, date_column='order_date') %}
    {#
    A reusable macro to generate a SQL WHERE clause fragment for date/timestamp filtering.

    This macro correctly handles four scenarios:
    1. Both dates provided (uses BETWEEN)
    2. Only start_date provided (uses >=)
    3. Only end_date provided (uses <=)
    4. Neither date provided (returns 1=1)

    Arguments:
      - start_date (string, optional): The beginning of the date range (e.g., '2024-01-01').
      - end_date (string, optional): The end of the date range (e.g., '2024-01-31').
      - date_column (string, optional): The name of the column to filter on (default: 'order_date').

    Usage Example (Requires project prefix):
    SELECT *
    FROM {{ ref('int_sales_enriched') }}
    WHERE {{ sales_analytics.custom_date_filter(
        start_date=var('start_date', none),
        date_column='event_timestamp'
    ) }}
    #}

    {%- if start_date and end_date -%}
        -- Scenario 1: Both dates provided
        {{ date_column }} BETWEEN '{{ start_date }}' AND '{{ end_date }}'

    {%- elif start_date -%}
        -- Scenario 2: Only start_date provided
        {{ date_column }} >= '{{ start_date }}'

    {%- elif end_date -%}
        -- Scenario 3: Only end_date provided
        {{ date_column }} <= '{{ end_date }}'

    {%- else -%}
        -- Scenario 4: No dates provided - pass all rows
        1=1

    {%- endif -%}

{% endmacro %}