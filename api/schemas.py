"""
Pydantic schemas for API request/response validation
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class ConfidenceLevel(str, Enum):
    """Confidence level for object detection"""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

class ObjectCategory(str, Enum):
    """Object categories for detected objects"""
    PEOPLE = "people"
    ELECTRONICS = "electronics"
    FURNITURE = "furniture"
    FOOD_DRINK = "food_drink"
    HOUSEHOLD_ITEMS = "household_items"
    VEHICLES = "vehicles"
    SPORTS = "sports"
    ANIMALS = "animals"
    OTHER = "other"

class FrequencyCategory(str, Enum):
    """Frequency categories for objects"""
    VERY_COMMON = "very_common"
    COMMON = "common"
    UNCOMMON = "uncommon"
    RARE = "rare"

class ReachCategory(str, Enum):
    """Message reach categories"""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    MINIMAL = "minimal"

# Base response model
class BaseResponse(BaseModel):
    """Base response model with common fields"""
    status: str = "success"
    message: str = "Request processed successfully"
    timestamp: datetime = Field(default_factory=datetime.now)

# Channel models
class ChannelInfo(BaseModel):
    """Channel information"""
    channel_name: str
    total_messages: int
    avg_views_per_message: float
    overall_forward_rate: float
    channel_activity_level: str

class ChannelActivity(BaseModel):
    """Channel activity details"""
    channel_name: str
    date: datetime
    message_count: int
    total_views: int
    total_forwards: int
    avg_engagement_score: float

class ChannelActivityResponse(BaseResponse):
    """Channel activity response"""
    data: List[ChannelActivity]
    total_records: int

# Message models
class MessageInfo(BaseModel):
    """Message information"""
    message_id: int
    channel_name: str
    message_date: datetime
    view_count: int
    forward_count: int
    reply_count: int
    engagement_score: float
    reach_category: ReachCategory
    message_text: Optional[str] = None
    has_media: bool

class MessageSearchResponse(BaseResponse):
    """Message search response"""
    data: List[MessageInfo]
    total_records: int
    query: str

# Product/Object detection models
class DetectedObject(BaseModel):
    """Detected object information"""
    object_class: str
    object_category: ObjectCategory
    total_detections: int
    messages_with_object: int
    channels_with_object: int
    avg_confidence: float
    frequency_category: FrequencyCategory
    importance_score: float

class TopProductsResponse(BaseResponse):
    """Top products response"""
    data: List[DetectedObject]
    total_records: int
    limit: int

class ObjectDetectionDetail(BaseModel):
    """Object detection detail"""
    detection_id: int
    message_id: int
    channel_name: str
    detected_object_class: str
    confidence_score: float
    confidence_level: ConfidenceLevel
    bbox_area: float
    detection_score: float
    detection_date: datetime
    message_date: datetime
    engagement_score: float

class ObjectDetectionResponse(BaseResponse):
    """Object detection response"""
    data: List[ObjectDetectionDetail]
    total_records: int

# Analytics models
class ChannelAnalytics(BaseModel):
    """Channel analytics summary"""
    channel_name: str
    total_messages: int
    total_views: int
    total_forwards: int
    total_replies: int
    avg_engagement_score: float
    top_detected_objects: List[str]
    detection_count: int
    avg_confidence: float

class ChannelAnalyticsResponse(BaseResponse):
    """Channel analytics response"""
    data: ChannelAnalytics

class DateRange(BaseModel):
    """Date range for filtering"""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

class TopChannelsResponse(BaseResponse):
    """Top channels response"""
    data: List[ChannelInfo]
    total_records: int

# Engagement models
class EngagementMetrics(BaseModel):
    """Engagement metrics"""
    date: datetime
    total_messages: int
    total_views: int
    total_forwards: int
    total_replies: int
    avg_engagement_score: float
    high_engagement_messages: int

class EngagementMetricsResponse(BaseResponse):
    """Engagement metrics response"""
    data: List[EngagementMetrics]
    total_records: int

# Error response
class ErrorResponse(BaseModel):
    """Error response model"""
    status: str = "error"
    message: str
    error_code: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)
