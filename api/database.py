"""
Database configuration and connection management
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'dbname': os.getenv('DB_NAME', 'kara_medical'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'your_secure_password')
}

class DatabaseManager:
    """Database connection manager with connection pooling"""
    
    def __init__(self):
        self.connection = None
    
    def get_connection(self):
        """Get database connection"""
        try:
            if self.connection is None or self.connection.closed:
                self.connection = psycopg2.connect(**DB_CONFIG)
                logger.info("✅ Database connection established")
            return self.connection
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def close_connection(self):
        """Close database connection"""
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("🔒 Database connection closed")
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute query and return results"""
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                if cursor.description:
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
                return []
        except Exception as e:
            logger.error(f"❌ Query execution failed: {e}")
            raise
    
    def execute_single_query(self, query: str, params: Optional[tuple] = None):
        """Execute query and return single result"""
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                if cursor.description:
                    result = cursor.fetchone()
                    return dict(result) if result else None
                return None
        except Exception as e:
            logger.error(f"❌ Single query execution failed: {e}")
            raise

# Global database manager instance
db_manager = DatabaseManager()
