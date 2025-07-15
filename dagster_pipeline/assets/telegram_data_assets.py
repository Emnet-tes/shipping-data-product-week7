"""
Telegram Data Assets for Dagster Pipeline
"""
import os
import pandas as pd
from dagster import asset, get_dagster_logger, AssetMaterialization, MetadataValue
from dagster import Output, MaterializeResult
import subprocess
import json
from typing import Dict, Any

@asset(
    description="Raw Telegram messages scraped from channels",
    compute_kind="python",
    group_name="telegram_data"
)
def raw_telegram_messages(context) -> MaterializeResult:
    """Scrape telegram messages and load into PostgreSQL"""
    logger = get_dagster_logger()
    
    try:
        # Run the telegram scraper (if exists) or data loader
        script_path = os.path.join(os.getcwd(), "scripts", "load_raw_messages.py")
        
        logger.info("📱 Loading raw Telegram messages")
        result = subprocess.run(
            f"python {script_path}",
            shell=True,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes
        )
        
        if result.returncode == 0:
            logger.info("✅ Raw Telegram messages loaded successfully")
            
            # Get count of loaded messages
            from ..resources import DB_CONFIG
            import psycopg2
            
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM raw.telegram_messages")
                    message_count = cur.fetchone()[0]
                    
                    # Get latest message date
                    cur.execute("SELECT MAX(date) FROM raw.telegram_messages")
                    latest_date = cur.fetchone()[0]
                    
                    # Get channel count
                    cur.execute("SELECT COUNT(DISTINCT channel) FROM raw.telegram_messages")
                    channel_count = cur.fetchone()[0]
            
            return MaterializeResult(
                metadata={
                    "message_count": MetadataValue.int(message_count),
                    "channel_count": MetadataValue.int(channel_count),
                    "latest_message_date": MetadataValue.text(str(latest_date)),
                    "load_status": MetadataValue.text("success"),
                    "output": MetadataValue.text(result.stdout[:1000])  # First 1000 chars
                }
            )
        else:
            logger.error(f"❌ Failed to load raw messages: {result.stderr}")
            return MaterializeResult(
                metadata={
                    "load_status": MetadataValue.text("failed"),
                    "error": MetadataValue.text(result.stderr),
                    "output": MetadataValue.text(result.stdout)
                }
            )
            
    except Exception as e:
        logger.error(f"❌ Error loading raw messages: {e}")
        return MaterializeResult(
            metadata={
                "load_status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )

@asset(
    description="Telegram images extracted from messages",
    compute_kind="filesystem",
    group_name="telegram_data",
    deps=[raw_telegram_messages]
)
def telegram_images(context) -> MaterializeResult:
    """Extract and organize telegram images"""
    logger = get_dagster_logger()
    
    try:
        # Check if images directory exists
        images_dir = os.path.join(os.getcwd(), "data", "raw", "telegram_images")
        
        if os.path.exists(images_dir):
            # Count images by walking through directory
            total_images = 0
            channels = set()
            
            for root, dirs, files in os.walk(images_dir):
                for file in files:
                    if file.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                        total_images += 1
                        # Extract channel from path
                        path_parts = root.split(os.sep)
                        if len(path_parts) > 2:
                            channels.add(path_parts[-1])
            
            logger.info(f"📸 Found {total_images} images across {len(channels)} channels")
            
            return MaterializeResult(
                metadata={
                    "total_images": MetadataValue.int(total_images),
                    "channel_count": MetadataValue.int(len(channels)),
                    "channels": MetadataValue.text(", ".join(channels)),
                    "images_directory": MetadataValue.text(images_dir),
                    "status": MetadataValue.text("success")
                }
            )
        else:
            logger.warning(f"📸 Images directory not found: {images_dir}")
            return MaterializeResult(
                metadata={
                    "total_images": MetadataValue.int(0),
                    "status": MetadataValue.text("directory_not_found"),
                    "images_directory": MetadataValue.text(images_dir)
                }
            )
            
    except Exception as e:
        logger.error(f"❌ Error processing telegram images: {e}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )

@asset(
    description="Data quality checks for raw telegram data",
    compute_kind="python",
    group_name="telegram_data",
    deps=[raw_telegram_messages]
)
def telegram_data_quality(context) -> MaterializeResult:
    """Perform data quality checks on raw telegram data"""
    logger = get_dagster_logger()
    
    try:
        from ..resources import DB_CONFIG
        import psycopg2
        
        quality_checks = {}
        
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Check for duplicates
                cur.execute("SELECT COUNT(*) - COUNT(DISTINCT id) as duplicates FROM raw.telegram_messages")
                quality_checks['duplicates'] = cur.fetchone()[0]
                
                # Check for null values in critical fields
                cur.execute("SELECT COUNT(*) FROM raw.telegram_messages WHERE channel IS NULL")
                quality_checks['null_channels'] = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM raw.telegram_messages WHERE date IS NULL")
                quality_checks['null_dates'] = cur.fetchone()[0]
                
                # Check date range
                cur.execute("SELECT MIN(date), MAX(date) FROM raw.telegram_messages")
                min_date, max_date = cur.fetchone()
                quality_checks['date_range'] = f"{min_date} to {max_date}"
                
                # Check for anomalies in view counts
                cur.execute("SELECT AVG(views), MAX(views), MIN(views) FROM raw.telegram_messages WHERE views IS NOT NULL")
                avg_views, max_views, min_views = cur.fetchone()
                quality_checks['views_stats'] = f"Avg: {avg_views:.0f}, Max: {max_views}, Min: {min_views}"
        
        # Determine overall quality score
        issues = quality_checks['duplicates'] + quality_checks['null_channels'] + quality_checks['null_dates']
        quality_score = max(0, 100 - (issues * 10))  # Deduct 10 points per issue
        
        logger.info(f"📊 Data quality score: {quality_score}/100")
        
        return MaterializeResult(
            metadata={
                "quality_score": MetadataValue.int(quality_score),
                "duplicates": MetadataValue.int(quality_checks['duplicates']),
                "null_channels": MetadataValue.int(quality_checks['null_channels']),
                "null_dates": MetadataValue.int(quality_checks['null_dates']),
                "date_range": MetadataValue.text(quality_checks['date_range']),
                "views_stats": MetadataValue.text(quality_checks['views_stats']),
                "status": MetadataValue.text("completed")
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Error in data quality checks: {e}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )
