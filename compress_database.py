import sqlite3
import sqlite_zstd
import os

# Paths to the original SQLite database
original_db_path = '/Users/khushikalra/ceesd/skvlite/pdict-v5-loopy-typed-and-scheduled-cache-v1-2024.1-islpy2023.2.5-cgen2020.1-507f856f4019474c17e48ad8ebfb302ee3898252-v13.11.9.final.0.sqlite'

# Output SQLite database path with zstd compression applied
compressed_db_path = '/Users/khushikalra/ceesd/skvlite/compressed_database.sqlite'

# Remove existing compressed database if it exists
if os.path.exists(compressed_db_path):
    os.remove(compressed_db_path)

# Connect to the original SQLite database
conn_original = sqlite3.connect(original_db_path)
cursor_original = conn_original.cursor()

# Connect to the new SQLite database with zstd compression enabled
conn_compressed = sqlite3.connect(compressed_db_path)
conn_compressed.enable_load_extension(True)
conn_compressed.execute("PRAGMA trusted_schema = OFF;")
sqlite_zstd.load(conn_compressed)
cursor_compressed = conn_compressed.cursor()

print("Initialized zstd extension.")

# Iterate over each table in the original database
cursor_original.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor_original.fetchall()

for table in tables:
    table_name = table[0]
    print(f"Processing table: {table_name}")
    
    # Get the CREATE TABLE SQL statement
    cursor_original.execute(f"SELECT sql FROM sqlite_master WHERE name='{table_name}';")
    create_table_sql = cursor_original.fetchone()[0]
    cursor_compressed.execute(create_table_sql)
    
    # Fetch all rows from the original table
    cursor_original.execute(f"SELECT * FROM {table_name};")
    rows = cursor_original.fetchall()
    
    # Get table columns information
    cursor_original.execute(f"PRAGMA table_info({table_name});")
    columns_info = cursor_original.fetchall()
    columns = [info[1] for info in columns_info]
    columns_str = ', '.join(columns)
    placeholders = ', '.join(['?' for _ in columns])
    
    # Insert data into the compressed database
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});"
    cursor_compressed.executemany(insert_sql, rows)
    conn_compressed.commit()
    print(f"Copied data for table: {table_name}")

    # Apply zstd compression to applicable columns
    for column_info in columns_info:
        column_name = column_info[1]
        column_type = column_info[2].lower()
        if column_type in ['text', 'blob'] and not column_info[5]:  # Skip if column is a PRIMARY KEY
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

# Perform incremental maintenance for zstd compression
cursor_compressed.execute("SELECT zstd_incremental_maintenance(null, 1);")
conn_compressed.commit()

# Optional: Perform a manual VACUUM to optimize the database size
cursor_compressed.execute("VACUUM;")
conn_compressed.commit()
print("Manual VACUUM completed.")

# Get the compressed database size
compressed_db_size = os.path.getsize(compressed_db_path)
print(f"Compressed database size: {compressed_db_size / 1024:.2f} KB")

# Close connections
conn_original.close()
conn_compressed.close()

print("Data transfer and compression complete.")
