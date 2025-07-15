import psycopg2

conn = psycopg2.connect(
    host='127.0.0.1',
    port=5432,
    dbname='kara_medical',
    user='postgres',
    password='your_secure_password'
)

cur = conn.cursor()

# Check null message_id records
cur.execute("""
    SELECT image_path, channel_name, message_id 
    FROM raw.image_detections 
    WHERE message_id IS NULL 
    LIMIT 10
""")
rows = cur.fetchall()

print("Sample null message_id records:")
for row in rows:
    print(f"  Path: {row[0]}, Channel: {row[1]}, Message ID: {row[2]}")

# Check if there's a pattern in the image paths
cur.execute("""
    SELECT DISTINCT channel_name, COUNT(*) as null_count
    FROM raw.image_detections 
    WHERE message_id IS NULL 
    GROUP BY channel_name
""")
channel_stats = cur.fetchall()

print("\nNull message_id by channel:")
for channel, count in channel_stats:
    print(f"  {channel}: {count}")

# Check if we can identify a pattern in the image paths
cur.execute("""
    SELECT image_path
    FROM raw.image_detections 
    WHERE message_id IS NULL 
    LIMIT 5
""")
paths = cur.fetchall()

print("\nSample image paths with null message_id:")
for path in paths:
    print(f"  {path[0]}")

conn.close()
