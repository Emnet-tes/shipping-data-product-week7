"""
Dagster Pipeline Definition for Telegram Data Analytics
"""
import os
import sys

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dagster import Definitions

# Import all assets with absolute imports
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

# Import jobs and schedules
from dagster_pipeline.jobs import ALL_JOBS
from dagster_pipeline.schedules import ALL_SCHEDULES

# Define all assets
all_assets = [
    # Telegram data assets
    raw_telegram_messages,
    telegram_images,
    telegram_data_quality,
    
    # dbt assets
    dbt_staging_models,
    dbt_dimension_tables,
    dbt_fact_tables,
    dbt_tests,
    
    # YOLO assets
    yolo_object_detection,
    yolo_dbt_models,
    yolo_data_quality,
    
    # FastAPI assets
    fastapi_service,
    api_endpoint_tests,
    api_performance_metrics
]

# Define all resources
all_resources = {
    "postgres_resource": postgres_resource,
    "dbt_resource": dbt_resource,
    "yolo_resource": yolo_resource,
    "telegram_scraper_resource": telegram_scraper_resource,
}

# Create Dagster definitions
defs = Definitions(
    assets=all_assets,
    resources=all_resources,
    jobs=ALL_JOBS,
    schedules=ALL_SCHEDULES,
)
