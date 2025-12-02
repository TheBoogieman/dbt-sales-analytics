# Helper script to export data from DuckDB to CSV
import duckdb
import sys

# Connect to database
conn = duckdb.connect('dev.duckdb')

# Get query from command line argument or use default
query = sys.argv[1] if len(sys.argv) > 1 else "COPY (   SELECT order_month_date, category_name, qoq_growth_rate   FROM main_marts.mart_seasonal_patterns   WHERE qoq_growth_rate IS NOT NULL) TO 'qoqtrend.csv' (HEADER, DELIMITER ','); "

# Execute and print results
result = conn.execute(query).fetchdf()
print(result.to_string())
conn.close()