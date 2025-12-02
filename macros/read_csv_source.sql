{# 
  Macro: read_csv_source
  Purpose: Read CSV files directly using DuckDB's read_csv_auto() function
  
  Parameters:
    - source_name: The source group name from _sources.yml
    - table_name: The specific table/file name
  
  Returns: SQL that DuckDB can execute to read the CSV
  
  Usage in models:
    {{ read_csv_source('raw_data', 'sales_fact') }}
#}

{% macro read_csv_source(source_name, table_name) %}
  
  {# Get the source metadata from _sources.yml #}
  {% set source_relation = source(source_name, table_name) %}
  
  {# Get the file path from meta property if it exists #}
  {% set src = source(source_name, table_name) %}
  {% set source_meta = src.meta %}
  
  {# Build the path to the CSV file #}
  {# Adjust this path based on where your files are relative to your project #}
  {% set csv_path = '../files/' ~ table_name ~ '.csv' %}
  
  {# Return DuckDB's read_csv_auto function #}
  read_csv_auto('{{ csv_path }}', header=true, delim=',', nullstr='')

{% endmacro %}