import duckdb
conn = duckdb.connect('dev.duckdb')

print('Top 5 customers by total spend:')
print(conn.execute('''
    SELECT 
        customer_name,
        total_orders,
        lifetime_net_amount,
        avg_order_value
    FROM main_intermediate.int_customer_aggregates
    ORDER BY lifetime_net_amount DESC
    LIMIT 5
''').fetchdf().to_string())

print('\n\nOrders distribution:')
print(conn.execute('''
    SELECT 
        total_orders,
        COUNT(*) as customer_count
    FROM main_intermediate.int_customer_aggregates
    GROUP BY total_orders
    ORDER BY total_orders
''').fetchdf().to_string())