import os
import json
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "kara_medical"
DB_USER = "postgres"
DB_PASSWORD = "your_secure_password"

RAW_DIR = Path("data/raw/telegram_messages/2025-07-14")

def connect_db():
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print(f"✅ Successfully connected to {DB_NAME}!")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        raise

def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;

            CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                id INT,
                channel TEXT,
                date TIMESTAMP,
                text TEXT,
                views INT,
                forwards INT,
                replies INT,
                has_media BOOLEAN,
                scraped_at TIMESTAMP
            );
        """)
        conn.commit()

def insert_messages(conn, messages):
    with conn.cursor() as cur:
        for msg in messages:
            cur.execute("""
                INSERT INTO raw.telegram_messages (
                    id, channel, date, text, views, forwards, replies, has_media, scraped_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                msg["id"],
                msg["channel"],
                msg["date"],
                msg["text"],
                msg["views"],
                msg["forwards"],
                msg["replies"],
                msg["has_media"],
                msg["scraped_at"]
            ))
        conn.commit()

def load_files():
    all_messages = []
    for file in RAW_DIR.glob("*.json"):
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            all_messages.extend(data["messages"])
    return all_messages

if __name__ == "__main__":
    conn = connect_db()
    create_table_if_not_exists(conn)

    print("📥 Loading JSON files...")
    messages = load_files()
    print(f"📦 Total messages to insert: {len(messages)}")

    insert_messages(conn, messages)
    conn.close()
    print("✅ Data loaded into raw.telegram_messages")
