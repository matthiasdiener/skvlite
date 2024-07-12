import sqlite3
import sqlite_zstd
import os

# Paths to the original and new SQLite databases
original_db_path = 'test.sqlite'
compressed_db_path = 'compressed_test.sqlite'

# Remove the compressed database if it already exists
if os.path.exists(compressed_db_path):
    os.remove(compressed_db_path)

# Step 2: Connect to the original SQLite database
conn_original = sqlite3.connect(original_db_path)
cursor_original = conn_original.cursor()

# Step 3: Create a new SQLite database with zstd compression
conn_compressed = sqlite3.connect(compressed_db_path)
conn_compressed.enable_load_extension(True)

# Add the following line to allow loading extensions
conn_compressed.execute("PRAGMA trusted_schema = OFF;")
sqlite_zstd.load(conn_compressed)  # Load the zstd extension
cursor_compressed = conn_compressed.cursor()

# Step 4: Read key-value pairs from the original database
cursor_original.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor_original.fetchall()

for table in tables:
    table_name = table[0]
    
    # Create the same table in the compressed database
    cursor_original.execute(f"SELECT sql FROM sqlite_master WHERE name='{table_name}';")
    create_table_sql = cursor_original.fetchone()[0]
    cursor_compressed.execute(create_table_sql)
    
    # Enable transparent compression for suitable text/blob columns
    cursor_original.execute(f"PRAGMA table_info({table_name});")
    columns_info = cursor_original.fetchall()
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
            except sqlite3.OperationalError as e:
                print(f"Error enabling compression for {table_name}.{column_name}: {str(e)}")
    
    # Copy data from the original table to the new compressed table
    cursor_original.execute(f"SELECT * FROM {table_name};")
    rows = cursor_original.fetchall()
    
    # Prepare the insert statement
    columns = [info[1] for info in columns_info]
    columns_str = ', '.join(columns)
    placeholders = ', '.join(['?' for _ in columns])
    
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});"
    
    cursor_compressed.executemany(insert_sql, rows)
    conn_compressed.commit()

# Perform incremental maintenance to compress data
cursor_compressed.execute("SELECT zstd_incremental_maintenance(null, 1);")
conn_compressed.commit()

# Step 6: Close the database connections
conn_original.close()
conn_compressed.close()

print("Data transfer to the compressed database is complete.")
