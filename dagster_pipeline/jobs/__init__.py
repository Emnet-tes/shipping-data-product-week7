"""
Dagster Jobs Definition for Complete Pipeline Orchestration
"""
import os
import sys

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dagster import job, op, get_dagster_logger, OpExecutionContext
from dagster import RunRequest, ScheduleEvaluationContext, DefaultScheduleStatus
from dagster import schedule, ScheduleDefinition
from dagster import define_asset_job, AssetSelection
from dagster import Definitions
from dagster import Config, AssetMaterialization
from typing import List

# Import all assets
from dagster_pipeline.assets.telegram_data_assets import (
    raw_telegram_messages,
    telegram_images,
    telegram_data_quality
)
from dagster_pipeline.assets.dbt_assets import (
    dbt_staging_models,
    dbt_dimension_tables,
    dbt_fact_tables,
    dbt_tests
)
from dagster_pipeline.assets.yolo_assets import (
    yolo_object_detection,
    yolo_dbt_models,
    yolo_data_quality
)
from dagster_pipeline.assets.fastapi_assets import (
    fastapi_service,
    api_endpoint_tests,
    api_performance_metrics
)

# Import resources
from dagster_pipeline.resources import (
    postgres_resource,
    dbt_resource,
    yolo_resource,
    telegram_scraper_resource
)

# Job 1: Data Ingestion Job
data_ingestion_job = define_asset_job(
    name="data_ingestion_job",
    selection=AssetSelection.assets(
        raw_telegram_messages,
        telegram_images,
        telegram_data_quality
    ),
    description="Ingest raw telegram data and perform initial quality checks",
    tags={"pipeline": "data_ingestion", "stage": "bronze"}
)

# Job 2: Data Transformation Job
data_transformation_job = define_asset_job(
    name="data_transformation_job",
    selection=AssetSelection.assets(
        dbt_staging_models,
        dbt_dimension_tables,
        dbt_fact_tables,
        dbt_tests
    ),
    description="Transform raw data into analytics-ready models using dbt",
    tags={"pipeline": "data_transformation", "stage": "silver"}
)

# Job 3: ML Enrichment Job
ml_enrichment_job = define_asset_job(
    name="ml_enrichment_job",
    selection=AssetSelection.assets(
        yolo_object_detection,
        yolo_dbt_models,
        yolo_data_quality
    ),
    description="Enrich data with YOLO object detection and transform for analytics",
    tags={"pipeline": "ml_enrichment", "stage": "gold"}
)

# Job 4: API Service Job
api_service_job = define_asset_job(
    name="api_service_job",
    selection=AssetSelection.assets(
        fastapi_service,
        api_endpoint_tests,
        api_performance_metrics
    ),
    description="Start and test FastAPI analytical service",
    tags={"pipeline": "api_service", "stage": "serving"}
)

# Job 5: Full Pipeline Job
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all(),
    description="Execute complete end-to-end pipeline",
    tags={"pipeline": "full", "stage": "complete"}
)

# Job 6: Quality Checks Job
quality_checks_job = define_asset_job(
    name="quality_checks_job",
    selection=AssetSelection.assets(
        telegram_data_quality,
        dbt_tests,
        yolo_data_quality,
        api_endpoint_tests
    ),
    description="Run all data quality and validation checks",
    tags={"pipeline": "quality", "stage": "validation"}
)

# Operational Jobs using @op decorator
@op(
    description="Health check for all pipeline components",
    tags={"type": "health_check"}
)
def pipeline_health_check(context: OpExecutionContext):
    """Perform health check on all pipeline components"""
    logger = get_dagster_logger()
    
    health_status = {
        "database": False,
        "dbt": False,
        "yolo": False,
        "api": False
    }
    
    try:
        # Check database connection
        import psycopg2
        DB_CONFIG = {
            "host": "127.0.0.1",
            "port": 5432,
            "database": "kara_medical",
            "user": "postgres",
            "password": "your_secure_password"
        }
        
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                health_status["database"] = True
        logger.info("✅ Database connection healthy")
        
    except Exception as e:
        logger.error(f"❌ Database health check failed: {e}")
    
    try:
        # Check dbt installation
        import subprocess
        result = subprocess.run("dbt --version", shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            health_status["dbt"] = True
            logger.info("✅ dbt installation healthy")
        else:
            logger.error("❌ dbt health check failed")
            
    except Exception as e:
        logger.error(f"❌ dbt health check failed: {e}")
    
    try:
        # Check YOLO/ultralytics
        import ultralytics
        health_status["yolo"] = True
        logger.info("✅ YOLO/ultralytics healthy")
        
    except Exception as e:
        logger.error(f"❌ YOLO health check failed: {e}")
    
    try:
        # Check API availability
        import requests
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            health_status["api"] = True
            logger.info("✅ API service healthy")
        else:
            logger.error("❌ API health check failed")
            
    except Exception as e:
        logger.error(f"❌ API health check failed: {e}")
    
    # Overall health score
    healthy_components = sum(health_status.values())
    total_components = len(health_status)
    health_score = (healthy_components / total_components) * 100
    
    context.log_event(
        AssetMaterialization(
            asset_key="pipeline_health_status",
            metadata={
                "health_score": health_score,
                "healthy_components": healthy_components,
                "total_components": total_components,
                "component_status": health_status
            }
        )
    )
    
    logger.info(f"📊 Pipeline health check completed. Score: {health_score:.1f}%")

@op(
    description="Clean up temporary files and optimize database",
    tags={"type": "maintenance"}
)
def pipeline_cleanup(context: OpExecutionContext):
    """Clean up temporary files and optimize database"""
    logger = get_dagster_logger()
    
    cleanup_results = {
        "temp_files_removed": 0,
        "database_optimized": False,
        "logs_rotated": False
    }
    
    try:
        # Clean up temporary image files
        import os
        import glob
        
        temp_patterns = [
            "temp_*.jpg",
            "temp_*.png",
            "*.tmp",
            "yolo_temp_*"
        ]
        
        for pattern in temp_patterns:
            for file_path in glob.glob(pattern):
                try:
                    os.remove(file_path)
                    cleanup_results["temp_files_removed"] += 1
                except:
                    pass
        
        logger.info(f"🧹 Cleaned up {cleanup_results['temp_files_removed']} temporary files")
        
        # Database optimization
        import psycopg2
        DB_CONFIG = {
            "host": "127.0.0.1",
            "port": 5432,
            "database": "kara_medical",
            "user": "postgres",
            "password": "your_secure_password"
        }
        
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Vacuum and analyze tables
                cur.execute("VACUUM ANALYZE")
                cleanup_results["database_optimized"] = True
        
        logger.info("🧹 Database optimization completed")
        
        # Log rotation (basic implementation)
        import shutil
        log_files = glob.glob("*.log")
        for log_file in log_files:
            if os.path.getsize(log_file) > 10 * 1024 * 1024:  # 10MB
                shutil.move(log_file, f"{log_file}.old")
        
        cleanup_results["logs_rotated"] = True
        logger.info("🧹 Log rotation completed")
        
    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
    
    logger.info("🧹 Pipeline cleanup completed")

# Health check job
@job(
    description="Pipeline health check and monitoring",
    tags={"type": "monitoring"}
)
def health_check_job():
    """Job to check pipeline health"""
    pipeline_health_check()

# Cleanup job
@job(
    description="Pipeline maintenance and cleanup",
    tags={"type": "maintenance"}
)
def cleanup_job():
    """Job to clean up and maintain pipeline"""
    pipeline_cleanup()

# Export all jobs
ALL_JOBS = [
    data_ingestion_job,
    data_transformation_job,
    ml_enrichment_job,
    api_service_job,
    full_pipeline_job,
    quality_checks_job,
    health_check_job,
    cleanup_job
]
