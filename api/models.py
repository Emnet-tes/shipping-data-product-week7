"""
Data models for the API
"""
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

@dataclass
class DatabaseConfig:
    """Database configuration"""
    host: str
    port: int
    dbname: str
    user: str
    password: str

@dataclass
class QueryResult:
    """Query result wrapper"""
    data: List[Dict[str, Any]]
    total_count: int
    execution_time: float
    query: str

class QueryType(Enum):
    """Query types for logging and monitoring"""
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    ANALYTICS = "ANALYTICS"

@dataclass
class AnalyticsQuery:
    """Analytics query model"""
    query_type: QueryType
    table_name: str
    filters: Optional[Dict[str, Any]] = None
    order_by: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None

@dataclass
class APIMetrics:
    """API performance metrics"""
    endpoint: str
    method: str
    response_time: float
    status_code: int
    timestamp: datetime
    user_agent: Optional[str] = None
