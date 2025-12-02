import duckdb; 

conn = duckdb.connect('dev.duckdb'); 
print(conn.execute('SHOW ALL TABLES').fetchdf())
print("Printing the SQL schemas:")
print(conn.execute('SELECT * FROM information_schema.schemata').fetchdf())