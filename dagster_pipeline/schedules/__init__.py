"""
Dagster Schedules for Automated Pipeline Execution
"""
import os
import sys

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dagster import schedule, ScheduleDefinition, RunRequest, ScheduleEvaluationContext
from dagster import DefaultScheduleStatus, SkipReason
from dagster import build_schedule_context
from dagster import DagsterLogManager
from datetime import datetime, timedelta

# Import jobs
from dagster_pipeline.jobs import (
    data_ingestion_job,
    data_transformation_job,
    ml_enrichment_job,
    api_service_job,
    full_pipeline_job,
    quality_checks_job,
    health_check_job,
    cleanup_job
)

# Schedule 1: Daily Full Pipeline
@schedule(
    job=full_pipeline_job,
    cron_schedule="0 2 * * *",  # 2 AM daily
    default_status=DefaultScheduleStatus.RUNNING,
    description="Execute complete pipeline daily at 2 AM"
)
def daily_full_pipeline_schedule(context: ScheduleEvaluationContext):
    """Daily execution of the complete pipeline"""
    
    # Check if it's a weekday (optional business logic)
    current_time = datetime.now()
    
    # Add run configuration
    run_config = {
        "resources": {
            "postgres_resource": {
                "config": {
                    "host": "127.0.0.1",
                    "port": 5432,
                    "database": "kara_medical",
                    "user": "postgres",
                    "password": "your_secure_password"
                }
            }
        }
    }
    
    return RunRequest(
        run_key=f"daily_full_pipeline_{current_time.strftime('%Y%m%d')}",
        run_config=run_config,
        tags={
            "schedule": "daily_full_pipeline",
            "date": current_time.strftime('%Y-%m-%d'),
            "type": "automated"
        }
    )

# Schedule 2: Hourly Data Ingestion
@schedule(
    job=data_ingestion_job,
    cron_schedule="0 * * * *",  # Every hour
    default_status=DefaultScheduleStatus.RUNNING,
    description="Ingest new telegram data every hour"
)
def hourly_data_ingestion_schedule(context: ScheduleEvaluationContext):
    """Hourly data ingestion from telegram"""
    
    current_time = datetime.now()
    
    # Skip during maintenance hours (1 AM - 3 AM)
    if 1 <= current_time.hour <= 3:
        return SkipReason("Skipping during maintenance hours")
    
    run_config = {
        "resources": {
            "telegram_scraper_resource": {
                "config": {
                    "batch_size": 100,
                    "timeout": 300
                }
            }
        }
    }
    
    return RunRequest(
        run_key=f"hourly_ingestion_{current_time.strftime('%Y%m%d_%H')}",
        run_config=run_config,
        tags={
            "schedule": "hourly_ingestion",
            "hour": current_time.strftime('%H'),
            "type": "automated"
        }
    )

# Schedule 3: Data Transformation (Every 4 hours)
@schedule(
    job=data_transformation_job,
    cron_schedule="0 */4 * * *",  # Every 4 hours
    default_status=DefaultScheduleStatus.RUNNING,
    description="Transform data using dbt every 4 hours"
)
def data_transformation_schedule(context: ScheduleEvaluationContext):
    """Data transformation every 4 hours"""
    
    current_time = datetime.now()
    
    run_config = {
        "resources": {
            "dbt_resource": {
                "config": {
                    "project_dir": os.path.join(os.getcwd(), "telegram_analytics"),
                    "profiles_dir": os.path.join(os.getcwd(), "telegram_analytics"),
                    "target": "dev"
                }
            }
        }
    }
    
    return RunRequest(
        run_key=f"transformation_{current_time.strftime('%Y%m%d_%H')}",
        run_config=run_config,
        tags={
            "schedule": "data_transformation",
            "interval": "4_hours",
            "type": "automated"
        }
    )

# Schedule 4: ML Enrichment (Every 6 hours)
@schedule(
    job=ml_enrichment_job,
    cron_schedule="0 */6 * * *",  # Every 6 hours
    default_status=DefaultScheduleStatus.RUNNING,
    description="Run YOLO object detection every 6 hours"
)
def ml_enrichment_schedule(context: ScheduleEvaluationContext):
    """ML enrichment every 6 hours"""
    
    current_time = datetime.now()
    
    # Skip if it's too early in the morning
    if current_time.hour < 6:
        return SkipReason("Skipping early morning ML processing")
    
    run_config = {
        "resources": {
            "yolo_resource": {
                "config": {
                    "model_path": "yolov8n.pt",
                    "confidence_threshold": 0.5,
                    "batch_size": 32
                }
            }
        }
    }
    
    return RunRequest(
        run_key=f"ml_enrichment_{current_time.strftime('%Y%m%d_%H')}",
        run_config=run_config,
        tags={
            "schedule": "ml_enrichment",
            "interval": "6_hours",
            "type": "automated"
        }
    )

# Schedule 5: Quality Checks (Every 2 hours)
@schedule(
    job=quality_checks_job,
    cron_schedule="30 */2 * * *",  # Every 2 hours at 30 minutes
    default_status=DefaultScheduleStatus.RUNNING,
    description="Run quality checks every 2 hours"
)
def quality_checks_schedule(context: ScheduleEvaluationContext):
    """Quality checks every 2 hours"""
    
    current_time = datetime.now()
    
    return RunRequest(
        run_key=f"quality_checks_{current_time.strftime('%Y%m%d_%H%M')}",
        tags={
            "schedule": "quality_checks",
            "interval": "2_hours",
            "type": "automated"
        }
    )

# Schedule 6: Health Checks (Every 15 minutes)
@schedule(
    job=health_check_job,
    cron_schedule="*/15 * * * *",  # Every 15 minutes
    default_status=DefaultScheduleStatus.RUNNING,
    description="Monitor pipeline health every 15 minutes"
)
def health_check_schedule(context: ScheduleEvaluationContext):
    """Health monitoring every 15 minutes"""
    
    current_time = datetime.now()
    
    return RunRequest(
        run_key=f"health_check_{current_time.strftime('%Y%m%d_%H%M')}",
        tags={
            "schedule": "health_check",
            "interval": "15_minutes",
            "type": "monitoring"
        }
    )

# Schedule 7: Cleanup (Weekly)
@schedule(
    job=cleanup_job,
    cron_schedule="0 3 * * 0",  # Sunday at 3 AM
    default_status=DefaultScheduleStatus.RUNNING,
    description="Weekly cleanup and maintenance"
)
def weekly_cleanup_schedule(context: ScheduleEvaluationContext):
    """Weekly cleanup and maintenance"""
    
    current_time = datetime.now()
    
    return RunRequest(
        run_key=f"weekly_cleanup_{current_time.strftime('%Y%m%d')}",
        tags={
            "schedule": "weekly_cleanup",
            "interval": "weekly",
            "type": "maintenance"
        }
    )

# Schedule 8: API Service Check (Every 30 minutes)
@schedule(
    job=api_service_job,
    cron_schedule="*/30 * * * *",  # Every 30 minutes
    default_status=DefaultScheduleStatus.RUNNING,
    description="Check and restart API service if needed"
)
def api_service_schedule(context: ScheduleEvaluationContext):
    """API service monitoring every 30 minutes"""
    
    current_time = datetime.now()
    
    return RunRequest(
        run_key=f"api_service_{current_time.strftime('%Y%m%d_%H%M')}",
        tags={
            "schedule": "api_service",
            "interval": "30_minutes",
            "type": "service_check"
        }
    )

# Conditional Schedule: Weekend Processing
@schedule(
    job=full_pipeline_job,
    cron_schedule="0 10 * * 6,0",  # Saturday and Sunday at 10 AM
    default_status=DefaultScheduleStatus.STOPPED,  # Disabled by default
    description="Weekend processing for catch-up"
)
def weekend_processing_schedule(context: ScheduleEvaluationContext):
    """Weekend processing for catch-up"""
    
    current_time = datetime.now()
    
    # Only run if it's actually weekend
    if current_time.weekday() not in [5, 6]:  # Saturday = 5, Sunday = 6
        return SkipReason("Not a weekend")
    
    run_config = {
        "resources": {
            "postgres_resource": {
                "config": {
                    "host": "127.0.0.1",
                    "port": 5432,
                    "database": "kara_medical",
                    "user": "postgres",
                    "password": "your_secure_password"
                }
            }
        }
    }
    
    return RunRequest(
        run_key=f"weekend_processing_{current_time.strftime('%Y%m%d')}",
        run_config=run_config,
        tags={
            "schedule": "weekend_processing",
            "day": current_time.strftime('%A'),
            "type": "catch_up"
        }
    )

# Export all schedules
ALL_SCHEDULES = [
    daily_full_pipeline_schedule,
    hourly_data_ingestion_schedule,
    data_transformation_schedule,
    ml_enrichment_schedule,
    quality_checks_schedule,
    health_check_schedule,
    weekly_cleanup_schedule,
    api_service_schedule,
    weekend_processing_schedule
]
