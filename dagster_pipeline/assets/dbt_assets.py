"""
dbt Transformation Assets for Dagster Pipeline
"""
import os
from dagster import asset, get_dagster_logger, MetadataValue, MaterializeResult
from dagster import AssetExecutionContext
import subprocess
import json
from typing import Dict, Any

@asset(
    description="dbt staging models for telegram data",
    compute_kind="dbt",
    group_name="dbt_transformations",
    deps=["raw_telegram_messages"]
)
def dbt_staging_models(context: AssetExecutionContext) -> MaterializeResult:
    """Run dbt staging models"""
    logger = get_dagster_logger()
    
    try:
        # Change to dbt project directory
        dbt_dir = os.path.join(os.getcwd(), "telegram_analytics")
        
        logger.info("🔧 Running dbt staging models")
        result = subprocess.run(
            "dbt run --select staging",
            shell=True,
            cwd=dbt_dir,
            capture_output=True,
            text=True,
            timeout=600
        )
        
        if result.returncode == 0:
            logger.info("✅ dbt staging models completed successfully")
            
            # Extract model count from output
            output_lines = result.stdout.split('\n')
            success_count = 0
            for line in output_lines:
                if "OK created" in line or "OK view" in line:
                    success_count += 1
            
            return MaterializeResult(
                metadata={
                    "models_built": MetadataValue.int(success_count),
                    "status": MetadataValue.text("success"),
                    "dbt_output": MetadataValue.text(result.stdout[-1000:])  # Last 1000 chars
                }
            )
        else:
            logger.error(f"❌ dbt staging models failed: {result.stderr}")
            return MaterializeResult(
                metadata={
                    "status": MetadataValue.text("failed"),
                    "error": MetadataValue.text(result.stderr),
                    "dbt_output": MetadataValue.text(result.stdout)
                }
            )
            
    except Exception as e:
        logger.error(f"❌ Error running dbt staging models: {e}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )

@asset(
    description="dbt dimension tables",
    compute_kind="dbt",
    group_name="dbt_transformations",
    deps=[dbt_staging_models]
)
def dbt_dimension_tables(context: AssetExecutionContext) -> MaterializeResult:
    """Run dbt dimension tables"""
    logger = get_dagster_logger()
    
    try:
        dbt_dir = os.path.join(os.getcwd(), "telegram_analytics")
        
        logger.info("🔧 Running dbt dimension tables")
        result = subprocess.run(
            "dbt run --select marts.dim_*",
            shell=True,
            cwd=dbt_dir,
            capture_output=True,
            text=True,
            timeout=600
        )
        
        if result.returncode == 0:
            logger.info("✅ dbt dimension tables completed successfully")
            
            # Get dimension table counts
            from ..resources import DB_CONFIG
            import psycopg2
            
            dim_counts = {}
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    # Count records in dimension tables
                    dim_tables = ['dim_channels', 'dim_dates', 'dim_objects']
                    for table in dim_tables:
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM analytics.{table}")
                            dim_counts[table] = cur.fetchone()[0]
                        except:
                            dim_counts[table] = 0
            
            return MaterializeResult(
                metadata={
                    "dim_channels_count": MetadataValue.int(dim_counts.get('dim_channels', 0)),
                    "dim_dates_count": MetadataValue.int(dim_counts.get('dim_dates', 0)),
                    "dim_objects_count": MetadataValue.int(dim_counts.get('dim_objects', 0)),
                    "status": MetadataValue.text("success"),
                    "dbt_output": MetadataValue.text(result.stdout[-1000:])
                }
            )
        else:
            logger.error(f"❌ dbt dimension tables failed: {result.stderr}")
            return MaterializeResult(
                metadata={
                    "status": MetadataValue.text("failed"),
                    "error": MetadataValue.text(result.stderr),
                    "dbt_output": MetadataValue.text(result.stdout)
                }
            )
            
    except Exception as e:
        logger.error(f"❌ Error running dbt dimension tables: {e}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )

@asset(
    description="dbt fact tables",
    compute_kind="dbt",
    group_name="dbt_transformations",
    deps=[dbt_dimension_tables]
)
def dbt_fact_tables(context: AssetExecutionContext) -> MaterializeResult:
    """Run dbt fact tables"""
    logger = get_dagster_logger()
    
    try:
        dbt_dir = os.path.join(os.getcwd(), "telegram_analytics")
        
        logger.info("🔧 Running dbt fact tables")
        result = subprocess.run(
            "dbt run --select marts.fct_*",
            shell=True,
            cwd=dbt_dir,
            capture_output=True,
            text=True,
            timeout=600
        )
        
        if result.returncode == 0:
            logger.info("✅ dbt fact tables completed successfully")
            
            # Get fact table counts
            from ..resources import DB_CONFIG
            import psycopg2
            
            fact_counts = {}
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    # Count records in fact tables
                    fact_tables = ['fct_messages', 'fct_image_detections']
                    for table in fact_tables:
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM analytics.{table}")
                            fact_counts[table] = cur.fetchone()[0]
                        except:
                            fact_counts[table] = 0
            
            return MaterializeResult(
                metadata={
                    "fct_messages_count": MetadataValue.int(fact_counts.get('fct_messages', 0)),
                    "fct_image_detections_count": MetadataValue.int(fact_counts.get('fct_image_detections', 0)),
                    "status": MetadataValue.text("success"),
                    "dbt_output": MetadataValue.text(result.stdout[-1000:])
                }
            )
        else:
            logger.error(f"❌ dbt fact tables failed: {result.stderr}")
            return MaterializeResult(
                metadata={
                    "status": MetadataValue.text("failed"),
                    "error": MetadataValue.text(result.stderr),
                    "dbt_output": MetadataValue.text(result.stdout)
                }
            )
            
    except Exception as e:
        logger.error(f"❌ Error running dbt fact tables: {e}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )

@asset(
    description="dbt test results",
    compute_kind="dbt",
    group_name="dbt_transformations",
    deps=[dbt_fact_tables]
)
def dbt_tests(context: AssetExecutionContext) -> MaterializeResult:
    """Run dbt tests"""
    logger = get_dagster_logger()
    
    try:
        dbt_dir = os.path.join(os.getcwd(), "telegram_analytics")
        
        logger.info("🧪 Running dbt tests")
        result = subprocess.run(
            "dbt test",
            shell=True,
            cwd=dbt_dir,
            capture_output=True,
            text=True,
            timeout=600
        )
        
        # dbt test returns non-zero if tests fail, but we want to capture results
        logger.info("✅ dbt tests completed")
        
        # Parse test results
        output_lines = result.stdout.split('\n')
        passed_tests = 0
        failed_tests = 0
        
        for line in output_lines:
            if "PASS" in line:
                passed_tests += 1
            elif "FAIL" in line or "ERROR" in line:
                failed_tests += 1
        
        total_tests = passed_tests + failed_tests
        success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
        
        return MaterializeResult(
            metadata={
                "total_tests": MetadataValue.int(total_tests),
                "passed_tests": MetadataValue.int(passed_tests),
                "failed_tests": MetadataValue.int(failed_tests),
                "success_rate": MetadataValue.float(success_rate),
                "status": MetadataValue.text("completed"),
                "dbt_output": MetadataValue.text(result.stdout[-1500:])  # Last 1500 chars
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Error running dbt tests: {e}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )
