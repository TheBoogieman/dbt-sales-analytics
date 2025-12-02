import duckdb; 

conn = duckdb.connect('dev.duckdb'); 
print('Intermediate Data Counts:')
print('int_customer_aggregates count:',conn.execute('SELECT COUNT(*) FROM main_intermediate.int_customer_aggregates').fetchone()[0])
print('int_sales_enriched count:',conn.execute('SELECT COUNT(*) FROM main_intermediate.int_sales_enriched').fetchone()[0])
print('Source Data Counts:')
print('stg_sales_fact:', conn.execute('SELECT COUNT(*) FROM main_staging.stg_sales_fact').fetchone()[0])
print('stg_customer:', conn.execute('SELECT COUNT(*) FROM main_staging.stg_customer').fetchone()[0])
print('stg_product:', conn.execute('SELECT COUNT(*) FROM main_staging.stg_product').fetchone()[0])
print('stg_product_category:', conn.execute('SELECT COUNT(*) FROM main_staging.stg_product_category').fetchone()[0])
