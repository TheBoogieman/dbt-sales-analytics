# Helper script to test and run arbitrary SQL queries against the DuckDB database
import duckdb
import sys

# Connect to database
conn = duckdb.connect('dev.duckdb')

# Get query from command line argument or use default
query = sys.argv[1] if len(sys.argv) > 1 else "SELECT * FROM main_staging.stg_sales_fact LIMIT 5"

# Execute and print results
result = conn.execute(query).fetchdf()
print(result.to_string())
conn.close()
