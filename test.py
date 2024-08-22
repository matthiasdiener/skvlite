import sqlite3
import sqlite_zstd
import os

# Create a new SQLite database
db_path = 'test.db'
if os.path.exists(db_path):
    os.remove(db_path)

conn = sqlite3.connect(db_path)
conn.enable_load_extension(True)
sqlite_zstd.load(conn)

# Create a table and insert some sample data
conn.execute('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)')
sample_data = "This is some sample data to test compression with sqlite-zstd. " * 100

for i in range(1, 101): #add data from the sqlite file - extract this data earlier 
    conn.execute('INSERT INTO test (data) VALUES (?)', (sample_data,))

conn.commit()

# Measure size after data insertion but before enabling compression
initial_size = os.path.getsize(db_path)

# Enable row-level compression on the 'data' column
compression_config = '{"table": "test", "column": "data", "compression_level": 19, "dict_chooser": "''a''"}'
conn.execute("SELECT zstd_enable_transparent('{\"table\": \"test\", \"column\": \"data\", \"compression_level\": 19, \"dict_chooser\": \"''a''\"}')")

# Perform incremental maintenance to compress the data
conn.execute('SELECT zstd_incremental_maintenance(null, 1)')
conn.commit()

# Measure size after enabling compression but before performing VACUUM
compressed_before_vacuum_size = os.path.getsize(db_path)

# Perform VACUUM to reclaim free space
conn.execute('VACUUM')
conn.commit()

# Measure size after performing VACUUM
final_compressed_size = os.path.getsize(db_path)

print(f"Size after data insertion: {initial_size} bytes")
print(f"Size after compression (before VACUUM): {compressed_before_vacuum_size} bytes")
print(f"Size after VACUUM: {final_compressed_size} bytes")

# Clean up
conn.close()
if os.path.exists(db_path):
    os.remove(db_path)
