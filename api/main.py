"""
FastAPI Analytical API for Telegram Data Analytics
"""
from fastapi import FastAPI, HTTPException, Query, Path, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List, Optional
import logging
from datetime import datetime

from .database import db_manager
from .crud import analytics_crud
from .schemas import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Telegram Analytics API",
    description="Analytical API for Telegram channel data with YOLO object detection insights",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Exception handlers
@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            message="Internal server error",
            error_code="INTERNAL_ERROR"
        ).dict()
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            message=exc.detail,
            error_code=f"HTTP_{exc.status_code}"
        ).dict()
    )

# Dependency for database connection
async def get_db():
    """Database dependency"""
    try:
        db_manager.get_connection()
        yield db_manager
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

# Health check endpoint
@app.get("/health", response_model=BaseResponse)
async def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        db_manager.execute_query("SELECT 1")
        return BaseResponse(
            message="API is healthy and database is accessible"
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail="Health check failed")

# Root endpoint
@app.get("/", response_model=BaseResponse)
async def root():
    """Root endpoint with API information"""
    return BaseResponse(
        message="Welcome to Telegram Analytics API! Visit /docs for API documentation."
    )

# API Routes

@app.get("/api/reports/top-products", response_model=TopProductsResponse)
async def get_top_products(
    limit: int = Query(10, ge=1, le=100, description="Number of top products to return"),
    db: db_manager = Depends(get_db)
):
    """
    Get the most frequently detected objects/products from images.
    
    This endpoint returns the top detected objects based on detection frequency,
    providing insights into what products or items are most commonly appearing
    in the telegram channel images.
    """
    try:
        products = analytics_crud.get_top_products(limit)
        
        return TopProductsResponse(
            data=products,
            total_records=len(products),
            limit=limit,
            message=f"Retrieved top {len(products)} products successfully"
        )
    except Exception as e:
        logger.error(f"Error in get_top_products: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve top products")

@app.get("/api/channels/{channel_name}/activity", response_model=ChannelActivityResponse)
async def get_channel_activity(
    channel_name: str = Path(..., description="Channel name to get activity for"),
    days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
    db: db_manager = Depends(get_db)
):
    """
    Get posting activity for a specific channel over time.
    
    This endpoint returns daily activity metrics for a channel including
    message count, views, forwards, and engagement scores.
    """
    try:
        activity = analytics_crud.get_channel_activity(channel_name, days)
        
        if not activity:
            raise HTTPException(
                status_code=404, 
                detail=f"Channel '{channel_name}' not found or has no activity"
            )
        
        return ChannelActivityResponse(
            data=activity,
            total_records=len(activity),
            message=f"Retrieved {len(activity)} days of activity for {channel_name}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_channel_activity: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve channel activity")

@app.get("/api/search/messages", response_model=MessageSearchResponse)
async def search_messages(
    query: str = Query(..., min_length=1, description="Search query for message content"),
    limit: int = Query(50, ge=1, le=200, description="Maximum number of results to return"),
    db: db_manager = Depends(get_db)
):
    """
    Search for messages containing specific keywords.
    
    This endpoint searches through message content and returns matching messages
    ranked by engagement score and view count.
    """
    try:
        messages = analytics_crud.search_messages(query, limit)
        
        return MessageSearchResponse(
            data=messages,
            total_records=len(messages),
            query=query,
            message=f"Found {len(messages)} messages matching '{query}'"
        )
    except Exception as e:
        logger.error(f"Error in search_messages: {e}")
        raise HTTPException(status_code=500, detail="Failed to search messages")

@app.get("/api/channels/{channel_name}/analytics", response_model=ChannelAnalyticsResponse)
async def get_channel_analytics(
    channel_name: str = Path(..., description="Channel name to get analytics for"),
    db: db_manager = Depends(get_db)
):
    """
    Get comprehensive analytics for a specific channel.
    
    This endpoint returns detailed analytics including message statistics,
    engagement metrics, and top detected objects for the channel.
    """
    try:
        analytics = analytics_crud.get_channel_analytics(channel_name)
        
        if not analytics:
            raise HTTPException(
                status_code=404,
                detail=f"Channel '{channel_name}' not found"
            )
        
        return ChannelAnalyticsResponse(
            data=analytics,
            message=f"Retrieved comprehensive analytics for {channel_name}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_channel_analytics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve channel analytics")

@app.get("/api/channels", response_model=TopChannelsResponse)
async def get_channels(
    limit: int = Query(10, ge=1, le=50, description="Number of channels to return"),
    db: db_manager = Depends(get_db)
):
    """
    Get list of channels ordered by activity.
    
    This endpoint returns channels with their basic statistics ordered by
    message count and activity level.
    """
    try:
        channels = analytics_crud.get_top_channels(limit)
        
        return TopChannelsResponse(
            data=channels,
            total_records=len(channels),
            message=f"Retrieved {len(channels)} channels successfully"
        )
    except Exception as e:
        logger.error(f"Error in get_channels: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve channels")

@app.get("/api/engagement/metrics", response_model=EngagementMetricsResponse)
async def get_engagement_metrics(
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    db: db_manager = Depends(get_db)
):
    """
    Get engagement metrics over time.
    
    This endpoint returns daily engagement metrics including message counts,
    views, forwards, replies, and engagement scores.
    """
    try:
        metrics = analytics_crud.get_engagement_metrics(days)
        
        return EngagementMetricsResponse(
            data=metrics,
            total_records=len(metrics),
            message=f"Retrieved {len(metrics)} days of engagement metrics"
        )
    except Exception as e:
        logger.error(f"Error in get_engagement_metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve engagement metrics")

@app.get("/api/detections", response_model=ObjectDetectionResponse)
async def get_object_detections(
    object_class: Optional[str] = Query(None, description="Filter by object class"),
    confidence_level: Optional[ConfidenceLevel] = Query(None, description="Filter by confidence level"),
    limit: int = Query(50, ge=1, le=200, description="Maximum number of results to return"),
    db: db_manager = Depends(get_db)
):
    """
    Get object detection results with optional filters.
    
    This endpoint returns YOLO object detection results with optional filtering
    by object class and confidence level.
    """
    try:
        detections = analytics_crud.get_object_detections(
            object_class=object_class,
            confidence_level=confidence_level.value if confidence_level else None,
            limit=limit
        )
        
        return ObjectDetectionResponse(
            data=detections,
            total_records=len(detections),
            message=f"Retrieved {len(detections)} object detections"
        )
    except Exception as e:
        logger.error(f"Error in get_object_detections: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve object detections")

# Additional utility endpoints

@app.get("/api/channels/list")
async def get_channel_list(db: db_manager = Depends(get_db)):
    """Get simple list of all channels"""
    try:
        channels = analytics_crud.get_channel_list()
        return {
            "channels": [channel["channel_name"] for channel in channels],
            "total_count": len(channels)
        }
    except Exception as e:
        logger.error(f"Error in get_channel_list: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve channel list")

@app.get("/api/stats/summary")
async def get_summary_stats(db: db_manager = Depends(get_db)):
    """Get summary statistics for the entire dataset"""
    try:
        # Get overall statistics
        stats_query = """
            SELECT 
                COUNT(DISTINCT dc.channel_name) as total_channels,
                COUNT(fm.message_id) as total_messages,
                SUM(fm.view_count) as total_views,
                SUM(fm.forward_count) as total_forwards,
                AVG(fm.engagement_score) as avg_engagement_score,
                COUNT(DISTINCT fid.detected_object_class) as unique_objects_detected,
                COUNT(fid.detection_id) as total_detections
            FROM analytics.fct_messages fm
            JOIN analytics.dim_channels dc ON fm.channel_key = dc.channel_key
            LEFT JOIN analytics.fct_image_detections fid ON fm.message_id = fid.message_id
        """
        
        stats = db_manager.execute_single_query(stats_query)
        
        return {
            "summary": stats,
            "message": "Summary statistics retrieved successfully"
        }
    except Exception as e:
        logger.error(f"Error in get_summary_stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve summary statistics")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
