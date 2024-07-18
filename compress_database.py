import sqlite3
import sqlite_zstd
import os
import random
import string

# Function to create random data
def create_random_data(num_rows):
    data = []
    for _ in range(num_rows):
        key = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=50))
        data.append((key, value))
    return data

# Paths to the original and new SQLite databases
original_db_path = 'test.sqlite'
compressed_db_path = 'compressed_test.sqlite'

if os.path.exists(original_db_path):
    os.remove(original_db_path)
if os.path.exists(compressed_db_path):
    os.remove(compressed_db_path)

conn_original = sqlite3.connect(original_db_path)
cursor_original = conn_original.cursor()

# Create a table and insert random data
cursor_original.execute('''
CREATE TABLE dict (
    key TEXT PRIMARY KEY,
    value TEXT
)
''')
random_data = create_random_data(1000)  
cursor_original.executemany('INSERT INTO dict (key, value) VALUES (?, ?)', random_data)
conn_original.commit()
conn_original.close()

print("Generated random database.")

# Connect to the original SQLite database
conn_original = sqlite3.connect(original_db_path)
cursor_original = conn_original.cursor()

# Create a new SQLite database with zstd compression
conn_compressed = sqlite3.connect(compressed_db_path)
conn_compressed.enable_load_extension(True)
conn_compressed.execute("PRAGMA trusted_schema = OFF;")
sqlite_zstd.load(conn_compressed)
cursor_compressed = conn_compressed.cursor()

print("Initialized zstd extension.")


conn_compressed.execute("PRAGMA journal_mode=WAL;")
conn_compressed.execute("PRAGMA auto_vacuum=full;")
print("Set PRAGMA journal_mode to WAL and auto_vacuum to full.")

cursor_original.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor_original.fetchall()

for table in tables:
    table_name = table[0]
    print(f"Processing table: {table_name}")
    
  
    cursor_original.execute(f"SELECT sql FROM sqlite_master WHERE name='{table_name}';")
    create_table_sql = cursor_original.fetchone()[0]
    cursor_compressed.execute(create_table_sql)
    
    # Copy data from the original table to the new compressed table
    cursor_original.execute(f"SELECT * FROM {table_name};")
    rows = cursor_original.fetchall()
    
   
    cursor_original.execute(f"PRAGMA table_info({table_name});")
    columns_info = cursor_original.fetchall()
    columns = [info[1] for info in columns_info]
    columns_str = ', '.join(columns)
    placeholders = ', '.join(['?' for _ in columns])
    
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});"
    
    for row in rows:
        try:
            cursor_compressed.execute(insert_sql, row)
        except sqlite3.OperationalError as e:
            print(f"Error inserting row into {table_name}: {str(e)}")
    conn_compressed.commit()
    print(f"Copied data for table: {table_name}")

    for column_info in columns_info:
        column_name = column_info[1]
        column_type = column_info[2].lower()
        if column_type in ['text', 'blob'] and not column_info[5]:  # Check if not part of primary key
            compression_config = f"""'{{
                "table": "{table_name}",
                "column": "{column_name}",
                "compression_level": 19,
                "dict_chooser": "''a''"
            }}'"""
            try:
                cursor_compressed.execute(f"SELECT zstd_enable_transparent({compression_config});")
                print(f"Enabled compression for {table_name}.{column_name}")
            except sqlite3.OperationalError as e:
                print(f"Error enabling compression for {table_name}.{column_name}: {str(e)}")


cursor_compressed.execute("SELECT zstd_incremental_maintenance(null, 1);")
conn_compressed.commit()


conn_original.close()
conn_compressed.close()
print("Data transfer to the compressed database is complete.")
