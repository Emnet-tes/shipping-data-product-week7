import psycopg2

conn = psycopg2.connect(
    host='127.0.0.1',
    port=5432,
    dbname='kara_medical',
    user='postgres',
    password='your_secure_password'
)

cur = conn.cursor()

# Get column names
cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'image_detections' 
    AND table_schema = 'raw' 
    ORDER BY ordinal_position
""")
columns = cur.fetchall()

print("Table structure:")
for col_name, col_type in columns:
    print(f"  {col_name}: {col_type}")

# Get sample data
cur.execute("SELECT * FROM raw.image_detections LIMIT 5")
rows = cur.fetchall()

print("\nSample data:")
for row in rows:
    print(row)

# Get summary statistics
cur.execute("""
    SELECT 
        COUNT(*) as total_detections,
        COUNT(DISTINCT message_id) as messages_with_detections,
        COUNT(DISTINCT detected_object_class) as unique_objects,
        AVG(confidence_score) as avg_confidence
    FROM raw.image_detections
""")
stats = cur.fetchone()
print(f"\nSummary Statistics:")
print(f"Total detections: {stats[0]}")
print(f"Messages with detections: {stats[1]}")
print(f"Unique object classes: {stats[2]}")
print(f"Average confidence: {stats[3]:.3f}")

# Get top detected objects
cur.execute("""
    SELECT detected_object_class, COUNT(*) as count
    FROM raw.image_detections
    GROUP BY detected_object_class
    ORDER BY count DESC
    LIMIT 10
""")
top_objects = cur.fetchall()

print("\nTop detected objects:")
for obj, count in top_objects:
    print(f"  {obj}: {count}")

conn.close()
