"""
YOLO Object Detection Assets for Dagster Pipeline
"""
import os
from dagster import asset, get_dagster_logger, MetadataValue, MaterializeResult
from dagster import AssetExecutionContext
import subprocess
import json
from typing import Dict, Any

@asset(
    description="YOLO object detection on telegram images",
    compute_kind="ml",
    group_name="yolo_enrichment",
    deps=["telegram_images", "dbt_staging_models"]
)
def yolo_object_detection(context: AssetExecutionContext) -> MaterializeResult:
    """Run YOLO object detection on telegram images"""
    logger = get_dagster_logger()
    
    try:
        # Run YOLO detection script
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
            
            # Get detection statistics
            DB_CONFIG = {
                "host": "127.0.0.1",
                "port": 5432,
                "database": "kara_medical",
                "user": "postgres",
                "password": "your_secure_password"
            }
            import psycopg2
            
            detection_stats = {}
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    # Total detections
                    cur.execute("SELECT COUNT(*) FROM raw.image_detections")
                    detection_stats['total_detections'] = cur.fetchone()[0]
                    
                    # Unique objects
                    cur.execute("SELECT COUNT(DISTINCT detected_object_class) FROM raw.image_detections")
                    detection_stats['unique_objects'] = cur.fetchone()[0]
                    
                    # Average confidence
                    cur.execute("SELECT AVG(confidence_score) FROM raw.image_detections")
                    detection_stats['avg_confidence'] = cur.fetchone()[0] or 0
                    
                    # Messages with detections
                    cur.execute("SELECT COUNT(DISTINCT message_id) FROM raw.image_detections WHERE message_id IS NOT NULL")
                    detection_stats['messages_with_detections'] = cur.fetchone()[0]
                    
                    # Top detected objects
                    cur.execute("""
                        SELECT detected_object_class, COUNT(*) as count
                        FROM raw.image_detections
                        GROUP BY detected_object_class
                        ORDER BY count DESC
                        LIMIT 5
                    """)
                    top_objects = cur.fetchall()
                    detection_stats['top_objects'] = [f"{obj[0]}: {obj[1]}" for obj in top_objects]
            
            return MaterializeResult(
                metadata={
                    "total_detections": MetadataValue.int(detection_stats['total_detections']),
                    "unique_objects": MetadataValue.int(detection_stats['unique_objects']),
                    "avg_confidence": MetadataValue.float(detection_stats['avg_confidence']),
                    "messages_with_detections": MetadataValue.int(detection_stats['messages_with_detections']),
                    "top_objects": MetadataValue.text("; ".join(detection_stats['top_objects'])),
                    "status": MetadataValue.text("success"),
                    "script_output": MetadataValue.text(result.stdout[-1000:])
                }
            )
        else:
            logger.error(f"❌ YOLO object detection failed: {result.stderr}")
            return MaterializeResult(
                metadata={
                    "status": MetadataValue.text("failed"),
                    "error": MetadataValue.text(result.stderr),
                    "script_output": MetadataValue.text(result.stdout)
                }
            )
            
    except Exception as e:
        logger.error(f"❌ Error running YOLO object detection: {e}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )

@asset(
    description="dbt models for YOLO detection data",
    compute_kind="dbt",
    group_name="yolo_enrichment",
    deps=["yolo_object_detection", "dbt_dimension_tables"]
)
def yolo_dbt_models(context: AssetExecutionContext) -> MaterializeResult:
    """Run dbt models for YOLO detection data"""
    logger = get_dagster_logger()
    
    try:
        dbt_dir = os.path.join(os.getcwd(), "telegram_analytics")
        
        logger.info("🔧 Running dbt models for YOLO data")
        result = subprocess.run(
            "dbt run --select stg_image_detections+ fct_image_detections+ dim_objects+",
            shell=True,
            cwd=dbt_dir,
            capture_output=True,
            text=True,
            timeout=600
        )
        
        if result.returncode == 0:
            logger.info("✅ dbt YOLO models completed successfully")
            
            # Get model counts
            DB_CONFIG = {
                "host": "127.0.0.1",
                "port": 5432,
                "database": "kara_medical",
                "user": "postgres",
                "password": "your_secure_password"
            }
            import psycopg2
            
            model_counts = {}
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    # Count records in YOLO-related tables
                    try:
                        cur.execute("SELECT COUNT(*) FROM analytics.stg_image_detections")
                        model_counts['stg_image_detections'] = cur.fetchone()[0]
                    except:
                        model_counts['stg_image_detections'] = 0
                    
                    try:
                        cur.execute("SELECT COUNT(*) FROM analytics.fct_image_detections")
                        model_counts['fct_image_detections'] = cur.fetchone()[0]
                    except:
                        model_counts['fct_image_detections'] = 0
                    
                    try:
                        cur.execute("SELECT COUNT(*) FROM analytics.dim_objects")
                        model_counts['dim_objects'] = cur.fetchone()[0]
                    except:
                        model_counts['dim_objects'] = 0
            
            return MaterializeResult(
                metadata={
                    "stg_image_detections_count": MetadataValue.int(model_counts['stg_image_detections']),
                    "fct_image_detections_count": MetadataValue.int(model_counts['fct_image_detections']),
                    "dim_objects_count": MetadataValue.int(model_counts['dim_objects']),
                    "status": MetadataValue.text("success"),
                    "dbt_output": MetadataValue.text(result.stdout[-1000:])
                }
            )
        else:
            logger.error(f"❌ dbt YOLO models failed: {result.stderr}")
            return MaterializeResult(
                metadata={
                    "status": MetadataValue.text("failed"),
                    "error": MetadataValue.text(result.stderr),
                    "dbt_output": MetadataValue.text(result.stdout)
                }
            )
            
    except Exception as e:
        logger.error(f"❌ Error running dbt YOLO models: {e}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )

@asset(
    description="YOLO data quality and validation checks",
    compute_kind="python",
    group_name="yolo_enrichment",
    deps=[yolo_dbt_models]
)
def yolo_data_quality(context: AssetExecutionContext) -> MaterializeResult:
    """Perform data quality checks on YOLO detection data"""
    logger = get_dagster_logger()
    
    try:
        DB_CONFIG = {
            "host": "127.0.0.1",
            "port": 5432,
            "database": "kara_medical",
            "user": "postgres",
            "password": "your_secure_password"
        }
        import psycopg2
        
        quality_checks = {}
        
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Check confidence score distribution
                cur.execute("""
                    SELECT 
                        COUNT(CASE WHEN confidence_score >= 0.8 THEN 1 END) as high_conf,
                        COUNT(CASE WHEN confidence_score >= 0.5 AND confidence_score < 0.8 THEN 1 END) as med_conf,
                        COUNT(CASE WHEN confidence_score < 0.5 THEN 1 END) as low_conf
                    FROM analytics.fct_image_detections
                """)
                conf_dist = cur.fetchone()
                quality_checks['confidence_distribution'] = {
                    'high': conf_dist[0],
                    'medium': conf_dist[1],
                    'low': conf_dist[2]
                }
                
                # Check for orphaned detections
                cur.execute("""
                    SELECT COUNT(*) FROM analytics.fct_image_detections fid
                    LEFT JOIN analytics.fct_messages fm ON fid.message_id = fm.message_id
                    WHERE fm.message_id IS NULL
                """)
                quality_checks['orphaned_detections'] = cur.fetchone()[0]
                
                # Check object category distribution
                cur.execute("""
                    SELECT object_category, COUNT(*) as count
                    FROM analytics.dim_objects
                    GROUP BY object_category
                    ORDER BY count DESC
                """)
                category_dist = cur.fetchall()
                quality_checks['category_distribution'] = [f"{cat[0]}: {cat[1]}" for cat in category_dist]
                
                # Check average detection score
                cur.execute("SELECT AVG(detection_score) FROM analytics.fct_image_detections")
                quality_checks['avg_detection_score'] = cur.fetchone()[0] or 0
        
        # Calculate quality score
        total_detections = sum(quality_checks['confidence_distribution'].values())
        high_conf_ratio = quality_checks['confidence_distribution']['high'] / total_detections if total_detections > 0 else 0
        orphaned_ratio = quality_checks['orphaned_detections'] / total_detections if total_detections > 0 else 0
        
        quality_score = int((high_conf_ratio * 50) + ((1 - orphaned_ratio) * 30) + 
                          (min(quality_checks['avg_detection_score'], 1.0) * 20))
        
        logger.info(f"📊 YOLO data quality score: {quality_score}/100")
        
        return MaterializeResult(
            metadata={
                "quality_score": MetadataValue.int(quality_score),
                "high_confidence_detections": MetadataValue.int(quality_checks['confidence_distribution']['high']),
                "medium_confidence_detections": MetadataValue.int(quality_checks['confidence_distribution']['medium']),
                "low_confidence_detections": MetadataValue.int(quality_checks['confidence_distribution']['low']),
                "orphaned_detections": MetadataValue.int(quality_checks['orphaned_detections']),
                "avg_detection_score": MetadataValue.float(quality_checks['avg_detection_score']),
                "category_distribution": MetadataValue.text("; ".join(quality_checks['category_distribution'])),
                "status": MetadataValue.text("completed")
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Error in YOLO data quality checks: {e}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )
