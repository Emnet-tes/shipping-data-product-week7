"""
Dagster Resources for Telegram Analytics Pipeline
"""
import os
import subprocess
from dagster import resource, get_dagster_logger
import psycopg2
from contextlib import contextmanager
from typing import Dict, Any

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'dbname': os.getenv('DB_NAME', 'kara_medical'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'your_secure_password')
}

@resource
def postgres_resource(context):
    """PostgreSQL database resource"""
    logger = get_dagster_logger()
    
    @contextmanager
    def get_connection():
        """Get database connection context manager"""
        conn = None
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            logger.info("✅ PostgreSQL connection established")
            yield conn
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.info("🔒 PostgreSQL connection closed")
    
    return get_connection

@resource
def dbt_resource(context):
    """dbt transformation resource"""
    logger = get_dagster_logger()
    
    def run_dbt_command(command: str, cwd: str = None) -> Dict[str, Any]:
        """Run dbt command and return result"""
        try:
            if cwd is None:
                cwd = os.path.join(os.getcwd(), "telegram_analytics")
            
            logger.info(f"🔧 Running dbt command: {command}")
            result = subprocess.run(
                f"dbt {command}",
                shell=True,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=600  # 10 minutes timeout
            )
            
            if result.returncode == 0:
                logger.info(f"✅ dbt command succeeded: {command}")
                return {
                    "success": True,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "command": command
                }
            else:
                logger.error(f"❌ dbt command failed: {command}")
                logger.error(f"Error output: {result.stderr}")
                return {
                    "success": False,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "command": command
                }
        except subprocess.TimeoutExpired:
            logger.error(f"⏰ dbt command timed out: {command}")
            return {
                "success": False,
                "error": "Command timed out",
                "command": command
            }
        except Exception as e:
            logger.error(f"❌ dbt command error: {e}")
            return {
                "success": False,
                "error": str(e),
                "command": command
            }
    
    return run_dbt_command

@resource
def yolo_resource(context):
    """YOLO object detection resource"""
    logger = get_dagster_logger()
    
    def run_yolo_detection(script_path: str = None) -> Dict[str, Any]:
        """Run YOLO object detection script"""
        try:
            if script_path is None:
                script_path = os.path.join(os.getcwd(), "scripts", "yolo_object_detection.py")
            
            logger.info("🤖 Starting YOLO object detection")
            result = subprocess.run(
                f"python {script_path}",
                shell=True,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            if result.returncode == 0:
                logger.info("✅ YOLO object detection completed successfully")
                return {
                    "success": True,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            else:
                logger.error("❌ YOLO object detection failed")
                logger.error(f"Error output: {result.stderr}")
                return {
                    "success": False,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
        except subprocess.TimeoutExpired:
            logger.error("⏰ YOLO detection timed out")
            return {
                "success": False,
                "error": "YOLO detection timed out"
            }
        except Exception as e:
            logger.error(f"❌ YOLO detection error: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    return run_yolo_detection

@resource
def telegram_scraper_resource(context):
    """Telegram scraper resource"""
    logger = get_dagster_logger()
    
    def run_telegram_scraper(script_path: str = None) -> Dict[str, Any]:
        """Run telegram scraper script"""
        try:
            if script_path is None:
                script_path = os.path.join(os.getcwd(), "scripts", "telegram_scraper.py")
            
            logger.info("📱 Starting Telegram data scraping")
            result = subprocess.run(
                f"python {script_path}",
                shell=True,
                capture_output=True,
                text=True,
                timeout=1800  # 30 minutes timeout
            )
            
            if result.returncode == 0:
                logger.info("✅ Telegram scraping completed successfully")
                return {
                    "success": True,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            else:
                logger.error("❌ Telegram scraping failed")
                logger.error(f"Error output: {result.stderr}")
                return {
                    "success": False,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
        except subprocess.TimeoutExpired:
            logger.error("⏰ Telegram scraping timed out")
            return {
                "success": False,
                "error": "Telegram scraping timed out"
            }
        except Exception as e:
            logger.error(f"❌ Telegram scraping error: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    return run_telegram_scraper
