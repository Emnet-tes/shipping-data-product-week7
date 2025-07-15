"""
FastAPI Analytical Service Assets for Dagster Pipeline
"""
import os
from dagster import asset, get_dagster_logger, MetadataValue, MaterializeResult
from dagster import AssetExecutionContext
import subprocess
import requests
import time
from typing import Dict, Any

@asset(
    description="Start FastAPI analytical service",
    compute_kind="api",
    group_name="api_service",
    deps=["dbt_fact_tables", "yolo_data_quality"]
)
def fastapi_service(context: AssetExecutionContext) -> MaterializeResult:
    """Start FastAPI analytical service"""
    logger = get_dagster_logger()
    
    try:
        # Check if API is already running
        try:
            response = requests.get("http://localhost:8000/docs", timeout=5)
            if response.status_code == 200:
                logger.info("🚀 FastAPI service is already running")
                return MaterializeResult(
                    metadata={
                        "status": MetadataValue.text("already_running"),
                        "api_url": MetadataValue.url("http://localhost:8000"),
                        "docs_url": MetadataValue.url("http://localhost:8000/docs")
                    }
                )
        except:
            pass
        
        # Start FastAPI service
        script_path = os.path.join(os.getcwd(), "scripts", "start_api.py")
        
        logger.info("🚀 Starting FastAPI analytical service")
        
        # Start API in background
        process = subprocess.Popen(
            f"python {script_path}",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for API to start
        max_retries = 30
        for i in range(max_retries):
            try:
                response = requests.get("http://localhost:8000/docs", timeout=2)
                if response.status_code == 200:
                    logger.info("✅ FastAPI service started successfully")
                    return MaterializeResult(
                        metadata={
                            "status": MetadataValue.text("started"),
                            "api_url": MetadataValue.url("http://localhost:8000"),
                            "docs_url": MetadataValue.url("http://localhost:8000/docs"),
                            "process_id": MetadataValue.int(process.pid),
                            "startup_time": MetadataValue.int(i + 1)
                        }
                    )
            except:
                time.sleep(1)
        
        logger.error("❌ FastAPI service failed to start")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("failed"),
                "error": MetadataValue.text("Service failed to start within timeout")
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Error starting FastAPI service: {e}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )

@asset(
    description="Test FastAPI endpoints",
    compute_kind="test",
    group_name="api_service",
    deps=[fastapi_service]
)
def api_endpoint_tests(context: AssetExecutionContext) -> MaterializeResult:
    """Test FastAPI endpoints"""
    logger = get_dagster_logger()
    
    try:
        base_url = "http://localhost:8000"
        
        # Test endpoints
        endpoints = [
            "/",
            "/health",
            "/api/v1/messages/stats",
            "/api/v1/messages/timeline",
            "/api/v1/channels/stats",
            "/api/v1/users/stats",
            "/api/v1/detection/stats",
            "/api/v1/detection/objects",
            "/api/v1/analytics/daily",
            "/api/v1/analytics/hourly",
            "/api/v1/analytics/top-channels",
            "/api/v1/analytics/top-users"
        ]
        
        test_results = {}
        successful_tests = 0
        
        for endpoint in endpoints:
            try:
                response = requests.get(f"{base_url}{endpoint}", timeout=10)
                test_results[endpoint] = {
                    "status_code": response.status_code,
                    "response_time": response.elapsed.total_seconds(),
                    "success": response.status_code == 200
                }
                
                if response.status_code == 200:
                    successful_tests += 1
                    logger.info(f"✅ {endpoint} - OK ({response.elapsed.total_seconds():.2f}s)")
                else:
                    logger.warning(f"⚠️ {endpoint} - Status {response.status_code}")
                    
            except Exception as e:
                test_results[endpoint] = {
                    "status_code": 0,
                    "response_time": 0,
                    "success": False,
                    "error": str(e)
                }
                logger.error(f"❌ {endpoint} - Error: {e}")
        
        # Calculate success rate
        success_rate = (successful_tests / len(endpoints)) * 100
        
        # Get average response time
        avg_response_time = sum(
            result["response_time"] for result in test_results.values() 
            if result["success"]
        ) / successful_tests if successful_tests > 0 else 0
        
        logger.info(f"📊 API Test Results: {successful_tests}/{len(endpoints)} endpoints passed ({success_rate:.1f}%)")
        
        return MaterializeResult(
            metadata={
                "total_endpoints": MetadataValue.int(len(endpoints)),
                "successful_tests": MetadataValue.int(successful_tests),
                "success_rate": MetadataValue.float(success_rate),
                "avg_response_time": MetadataValue.float(avg_response_time),
                "test_results": MetadataValue.json(test_results),
                "status": MetadataValue.text("completed")
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Error testing API endpoints: {e}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )

@asset(
    description="API performance metrics",
    compute_kind="monitoring",
    group_name="api_service",
    deps=[api_endpoint_tests]
)
def api_performance_metrics(context: AssetExecutionContext) -> MaterializeResult:
    """Collect API performance metrics"""
    logger = get_dagster_logger()
    
    try:
        base_url = "http://localhost:8000"
        
        # Performance test endpoints
        performance_endpoints = [
            "/api/v1/messages/stats",
            "/api/v1/analytics/daily",
            "/api/v1/detection/stats"
        ]
        
        performance_results = {}
        
        for endpoint in performance_endpoints:
            try:
                # Run multiple requests to get average
                response_times = []
                for i in range(5):
                    response = requests.get(f"{base_url}{endpoint}", timeout=10)
                    if response.status_code == 200:
                        response_times.append(response.elapsed.total_seconds())
                
                if response_times:
                    performance_results[endpoint] = {
                        "avg_response_time": sum(response_times) / len(response_times),
                        "min_response_time": min(response_times),
                        "max_response_time": max(response_times),
                        "requests_tested": len(response_times)
                    }
                    
                    logger.info(f"📈 {endpoint} - Avg: {performance_results[endpoint]['avg_response_time']:.3f}s")
                
            except Exception as e:
                logger.error(f"❌ Performance test failed for {endpoint}: {e}")
        
        # Calculate overall performance score
        if performance_results:
            avg_response_time = sum(
                result["avg_response_time"] for result in performance_results.values()
            ) / len(performance_results)
            
            # Performance score (lower response time = higher score)
            performance_score = max(0, min(100, int(100 - (avg_response_time * 50))))
        else:
            performance_score = 0
            avg_response_time = 0
        
        logger.info(f"📊 API Performance Score: {performance_score}/100")
        
        return MaterializeResult(
            metadata={
                "performance_score": MetadataValue.int(performance_score),
                "avg_response_time": MetadataValue.float(avg_response_time),
                "endpoints_tested": MetadataValue.int(len(performance_results)),
                "performance_details": MetadataValue.json(performance_results),
                "status": MetadataValue.text("completed")
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Error collecting API performance metrics: {e}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )
