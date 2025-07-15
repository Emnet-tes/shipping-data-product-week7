"""
YOLO Object Detection Script for Telegram Images
This script processes images scraped from Telegram channels using YOLOv8
and stores detection results in the database.
"""

import os
import json
import psycopg2
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import logging
from ultralytics import YOLO
from PIL import Image
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "kara_medical"
DB_USER = "postgres"
DB_PASSWORD = "your_secure_password"

# Image processing configuration
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff', '.webp'}
CONFIDENCE_THRESHOLD = 0.3
IMAGES_DIR = Path("data/raw/telegram_images")


class ImageDetectionProcessor:
    """Handles YOLO object detection for Telegram images"""
    
    def __init__(self):
        self.db_conn = None
        self.yolo_model = None
        self.processed_images = set()
        
    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            logger.info(f"✅ Connected to database {DB_NAME}")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def initialize_yolo(self):
        """Initialize YOLOv8 model"""
        try:
            logger.info("🚀 Loading YOLOv8 model...")
            self.yolo_model = YOLO('yolov8n.pt')  # Using nano model for speed
            logger.info("✅ YOLOv8 model loaded successfully")
        except Exception as e:
            logger.error(f"❌ Failed to load YOLO model: {e}")
            raise
    
    def create_detection_table(self):
        """Create table for storing object detection results"""
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS raw.image_detections (
                id SERIAL PRIMARY KEY,
                image_path TEXT NOT NULL,
                image_hash VARCHAR(64) NOT NULL,
                message_id INTEGER,
                channel_name TEXT,
                detected_object_class TEXT NOT NULL,
                confidence_score FLOAT NOT NULL,
                bbox_x1 FLOAT,
                bbox_y1 FLOAT,
                bbox_x2 FLOAT,
                bbox_y2 FLOAT,
                detection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                model_version TEXT DEFAULT 'yolov8n'
            );
            
            CREATE INDEX IF NOT EXISTS idx_image_detections_message_id 
            ON raw.image_detections(message_id);
            
            CREATE INDEX IF NOT EXISTS idx_image_detections_image_hash 
            ON raw.image_detections(image_hash);
            
            CREATE INDEX IF NOT EXISTS idx_image_detections_object_class 
            ON raw.image_detections(detected_object_class);
        """
        
        try:
            with self.db_conn.cursor() as cur:
                cur.execute(create_table_sql)
                self.db_conn.commit()
            logger.info("✅ Image detections table created/verified")
        except Exception as e:
            logger.error(f"❌ Failed to create detections table: {e}")
            raise
    
    def get_image_hash(self, image_path: Path) -> str:
        """Generate hash for image to avoid reprocessing"""
        try:
            with open(image_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            logger.error(f"❌ Failed to hash image {image_path}: {e}")
            return ""
    
    def is_image_processed(self, image_hash: str) -> bool:
        """Check if image has already been processed"""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM raw.image_detections WHERE image_hash = %s",
                    (image_hash,)
                )
                count = cur.fetchone()[0]
                return count > 0
        except Exception as e:
            logger.error(f"❌ Failed to check if image processed: {e}")
            return False
    
    def get_message_id_for_image(self, image_path: Path) -> tuple:
        """Extract message ID and channel from image path"""
        try:
            # Extract channel name from path
            path_parts = image_path.parts
            channel_name = None
            
            for part in path_parts:
                if part in ['CheMed123', 'lobelia4cosmetics', 'tikvahpharma']:
                    channel_name = part
                    break
            
            if not channel_name:
                return None, None
            
            # Try to extract message ID from filename
            filename = image_path.stem
            # Look for patterns like "message_123" or just numbers
            import re
            match = re.search(r'(\d+)', filename)
            if match:
                potential_message_id = int(match.group(1))
                
                # Verify this message ID exists in our database
                with self.db_conn.cursor() as cur:
                    cur.execute(
                        "SELECT id FROM raw.telegram_messages WHERE id = %s AND channel = %s",
                        (potential_message_id, channel_name)
                    )
                    result = cur.fetchone()
                    if result:
                        return potential_message_id, channel_name
            
            return None, channel_name
        except Exception as e:
            logger.error(f"❌ Failed to get message ID for {image_path}: {e}")
            return None, None
    
    def process_image(self, image_path: Path) -> List[Dict[str, Any]]:
        """Process single image with YOLO detection"""
        try:
            # Get image hash
            image_hash = self.get_image_hash(image_path)
            if not image_hash:
                return []
            
            # Check if already processed
            if self.is_image_processed(image_hash):
                logger.info(f"⏭️  Skipping already processed image: {image_path.name}")
                return []
            
            # Get message ID and channel
            message_id, channel_name = self.get_message_id_for_image(image_path)
            
            # Run YOLO detection
            results = self.yolo_model(str(image_path), conf=CONFIDENCE_THRESHOLD)
            
            detections = []
            for result in results:
                boxes = result.boxes
                if boxes is not None:
                    for box in boxes:
                        # Extract detection data
                        detection = {
                            'image_path': str(image_path),
                            'image_hash': image_hash,
                            'message_id': message_id,
                            'channel_name': channel_name,
                            'detected_object_class': result.names[int(box.cls[0])],
                            'confidence_score': float(box.conf[0]),
                            'bbox_x1': float(box.xyxy[0][0]),
                            'bbox_y1': float(box.xyxy[0][1]),
                            'bbox_x2': float(box.xyxy[0][2]),
                            'bbox_y2': float(box.xyxy[0][3])
                        }
                        detections.append(detection)
            
            logger.info(f"🔍 Found {len(detections)} objects in {image_path.name}")
            return detections
            
        except Exception as e:
            logger.error(f"❌ Failed to process image {image_path}: {e}")
            return []
    
    def save_detections(self, detections: List[Dict[str, Any]]):
        """Save detection results to database"""
        if not detections:
            return
        
        insert_sql = """
            INSERT INTO raw.image_detections (
                image_path, image_hash, message_id, channel_name,
                detected_object_class, confidence_score,
                bbox_x1, bbox_y1, bbox_x2, bbox_y2
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            with self.db_conn.cursor() as cur:
                for detection in detections:
                    cur.execute(insert_sql, (
                        detection['image_path'],
                        detection['image_hash'],
                        detection['message_id'],
                        detection['channel_name'],
                        detection['detected_object_class'],
                        detection['confidence_score'],
                        detection['bbox_x1'],
                        detection['bbox_y1'],
                        detection['bbox_x2'],
                        detection['bbox_y2']
                    ))
                self.db_conn.commit()
            logger.info(f"✅ Saved {len(detections)} detections to database")
        except Exception as e:
            logger.error(f"❌ Failed to save detections: {e}")
            self.db_conn.rollback()
    
    def find_images(self) -> List[Path]:
        """Find all images in the telegram images directory"""
        images = []
        
        if not IMAGES_DIR.exists():
            logger.warning(f"⚠️  Images directory not found: {IMAGES_DIR}")
            return images
        
        for image_path in IMAGES_DIR.rglob("*"):
            if image_path.is_file() and image_path.suffix.lower() in IMAGE_EXTENSIONS:
                images.append(image_path)
        
        logger.info(f"📁 Found {len(images)} images to process")
        return images
    
    def process_all_images(self):
        """Process all images in the directory"""
        images = self.find_images()
        
        if not images:
            logger.info("🔍 No images found to process")
            return
        
        processed_count = 0
        total_detections = 0
        
        for image_path in images:
            try:
                detections = self.process_image(image_path)
                if detections:
                    self.save_detections(detections)
                    total_detections += len(detections)
                    processed_count += 1
            except Exception as e:
                logger.error(f"❌ Error processing {image_path}: {e}")
                continue
        
        logger.info(f"🎯 Processing complete:")
        logger.info(f"   📊 Images processed: {processed_count}/{len(images)}")
        logger.info(f"   🔍 Total detections: {total_detections}")
    
    def get_detection_summary(self):
        """Get summary of detection results"""
        try:
            with self.db_conn.cursor() as cur:
                # Total detections
                cur.execute("SELECT COUNT(*) FROM raw.image_detections")
                total_detections = cur.fetchone()[0]
                
                # Most detected objects
                cur.execute("""
                    SELECT detected_object_class, COUNT(*) as count
                    FROM raw.image_detections
                    GROUP BY detected_object_class
                    ORDER BY count DESC
                    LIMIT 10
                """)
                top_objects = cur.fetchall()
                
                # Detections by channel
                cur.execute("""
                    SELECT channel_name, COUNT(*) as count
                    FROM raw.image_detections
                    WHERE channel_name IS NOT NULL
                    GROUP BY channel_name
                    ORDER BY count DESC
                """)
                by_channel = cur.fetchall()
                
                logger.info(f"📊 Detection Summary:")
                logger.info(f"   Total detections: {total_detections}")
                logger.info(f"   Top objects detected:")
                for obj, count in top_objects:
                    logger.info(f"     - {obj}: {count}")
                logger.info(f"   By channel:")
                for channel, count in by_channel:
                    logger.info(f"     - {channel}: {count}")
                    
        except Exception as e:
            logger.error(f"❌ Failed to get detection summary: {e}")
    
    def close(self):
        """Close database connection"""
        if self.db_conn:
            self.db_conn.close()
            logger.info("🔒 Database connection closed")


def main():
    """Main execution function"""
    processor = ImageDetectionProcessor()
    
    try:
        # Initialize components
        processor.connect_db()
        processor.initialize_yolo()
        processor.create_detection_table()
        
        # Process all images
        processor.process_all_images()
        
        # Show summary
        processor.get_detection_summary()
        
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        return 1
    
    finally:
        processor.close()
    
    return 0


if __name__ == "__main__":
    exit(main())
