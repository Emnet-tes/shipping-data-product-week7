"""
CRUD operations for analytical queries
"""
from typing import List, Optional, Dict, Any
from datetime import datetime
from .database import db_manager
from .schemas import *
import logging

logger = logging.getLogger(__name__)

class AnalyticsCRUD:
    """CRUD operations for analytics queries"""
    
    def __init__(self):
        self.db = db_manager
    
    def get_top_products(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top detected objects/products"""
        query = """
            SELECT 
                detected_object_class as object_class,
                object_category,
                total_detections,
                messages_with_object,
                channels_with_object,
                avg_confidence,
                frequency_category,
                importance_score
            FROM analytics.dim_objects
            ORDER BY total_detections DESC
            LIMIT %s
        """
        try:
            return self.db.execute_query(query, (limit,))
        except Exception as e:
            logger.error(f"Error getting top products: {e}")
            raise
    
    def get_channel_activity(self, channel_name: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get channel activity over time"""
        query = """
            SELECT 
                dc.channel_name,
                dd.date_day as date,
                COUNT(fm.message_id) as message_count,
                SUM(fm.view_count) as total_views,
                SUM(fm.forward_count) as total_forwards,
                AVG(fm.engagement_score) as avg_engagement_score
            FROM analytics.fct_messages fm
            JOIN analytics.dim_channels dc ON fm.channel_key = dc.channel_key
            JOIN analytics.dim_dates dd ON fm.date_key = dd.date_key
            WHERE dc.channel_name = %s
            AND dd.date_day >= CURRENT_DATE - INTERVAL '%s days'
            GROUP BY dc.channel_name, dd.date_day
            ORDER BY dd.date_day DESC
        """
        try:
            return self.db.execute_query(query, (channel_name, days))
        except Exception as e:
            logger.error(f"Error getting channel activity: {e}")
            raise
    
    def search_messages(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search messages containing specific keywords"""
        search_query = """
            SELECT 
                fm.message_id,
                dc.channel_name,
                fm.message_date,
                fm.view_count,
                fm.forward_count,
                fm.reply_count,
                fm.engagement_score,
                fm.reach_category,
                tm.text as message_text,
                fm.has_media
            FROM analytics.fct_messages fm
            JOIN analytics.dim_channels dc ON fm.channel_key = dc.channel_key
            JOIN raw.telegram_messages tm ON fm.message_id = tm.id
            WHERE LOWER(tm.text) LIKE LOWER(%s)
            ORDER BY fm.engagement_score DESC, fm.view_count DESC
            LIMIT %s
        """
        try:
            search_term = f"%{query}%"
            return self.db.execute_query(search_query, (search_term, limit))
        except Exception as e:
            logger.error(f"Error searching messages: {e}")
            raise
    
    def get_channel_analytics(self, channel_name: str) -> Dict[str, Any]:
        """Get comprehensive channel analytics"""
        query = """
            SELECT 
                dc.channel_name,
                dc.total_messages,
                SUM(fm.view_count) as total_views,
                SUM(fm.forward_count) as total_forwards,
                SUM(fm.reply_count) as total_replies,
                AVG(fm.engagement_score) as avg_engagement_score,
                dc.avg_views_per_message,
                dc.overall_forward_rate,
                dc.channel_activity_level
            FROM analytics.dim_channels dc
            LEFT JOIN analytics.fct_messages fm ON dc.channel_key = fm.channel_key
            WHERE dc.channel_name = %s
            GROUP BY dc.channel_key, dc.channel_name, dc.total_messages, 
                     dc.avg_views_per_message, dc.overall_forward_rate, 
                     dc.channel_activity_level
        """
        
        # Get top objects for this channel
        objects_query = """
            SELECT 
                fid.detected_object_class,
                COUNT(*) as detection_count,
                AVG(fid.confidence_score) as avg_confidence
            FROM analytics.fct_image_detections fid
            JOIN analytics.dim_channels dc ON fid.channel_key = dc.channel_key
            WHERE dc.channel_name = %s
            GROUP BY fid.detected_object_class
            ORDER BY detection_count DESC
            LIMIT 5
        """
        
        try:
            # Get main analytics
            main_result = self.db.execute_single_query(query, (channel_name,))
            if not main_result:
                return None
            
            # Get object detections
            objects_result = self.db.execute_query(objects_query, (channel_name,))
            
            # Combine results
            main_result['top_detected_objects'] = [obj['detected_object_class'] for obj in objects_result]
            main_result['detection_count'] = sum(obj['detection_count'] for obj in objects_result)
            main_result['avg_confidence'] = sum(obj['avg_confidence'] for obj in objects_result) / len(objects_result) if objects_result else 0.0
            
            return main_result
        except Exception as e:
            logger.error(f"Error getting channel analytics: {e}")
            raise
    
    def get_top_channels(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top channels by activity"""
        query = """
            SELECT 
                channel_name,
                total_messages,
                avg_views_per_message,
                overall_forward_rate,
                channel_activity_level
            FROM analytics.dim_channels
            ORDER BY total_messages DESC
            LIMIT %s
        """
        try:
            return self.db.execute_query(query, (limit,))
        except Exception as e:
            logger.error(f"Error getting top channels: {e}")
            raise
    
    def get_engagement_metrics(self, days: int = 30) -> List[Dict[str, Any]]:
        """Get engagement metrics over time"""
        query = """
            SELECT 
                dd.date_day as date,
                COUNT(fm.message_id) as total_messages,
                SUM(fm.view_count) as total_views,
                SUM(fm.forward_count) as total_forwards,
                SUM(fm.reply_count) as total_replies,
                AVG(fm.engagement_score) as avg_engagement_score,
                COUNT(CASE WHEN fm.engagement_score > 0.5 THEN 1 END) as high_engagement_messages
            FROM analytics.fct_messages fm
            JOIN analytics.dim_dates dd ON fm.date_key = dd.date_key
            WHERE dd.date_day >= CURRENT_DATE - INTERVAL '%s days'
            GROUP BY dd.date_day
            ORDER BY dd.date_day DESC
        """
        try:
            return self.db.execute_query(query, (days,))
        except Exception as e:
            logger.error(f"Error getting engagement metrics: {e}")
            raise
    
    def get_object_detections(self, object_class: Optional[str] = None, 
                            confidence_level: Optional[str] = None,
                            limit: int = 50) -> List[Dict[str, Any]]:
        """Get object detection details with filters"""
        base_query = """
            SELECT 
                fid.detection_id,
                fid.message_id,
                dc.channel_name,
                fid.detected_object_class,
                fid.confidence_score,
                fid.confidence_level,
                fid.bbox_area,
                fid.detection_score,
                fid.detection_date,
                fid.message_date,
                fid.engagement_score
            FROM analytics.fct_image_detections fid
            JOIN analytics.dim_channels dc ON fid.channel_key = dc.channel_key
            WHERE 1=1
        """
        
        params = []
        
        if object_class:
            base_query += " AND fid.detected_object_class = %s"
            params.append(object_class)
        
        if confidence_level:
            base_query += " AND fid.confidence_level = %s"
            params.append(confidence_level)
        
        base_query += " ORDER BY fid.detection_score DESC, fid.confidence_score DESC LIMIT %s"
        params.append(limit)
        
        try:
            return self.db.execute_query(base_query, tuple(params))
        except Exception as e:
            logger.error(f"Error getting object detections: {e}")
            raise
    
    def get_channel_list(self) -> List[Dict[str, Any]]:
        """Get list of all channels"""
        query = """
            SELECT 
                channel_name,
                total_messages,
                avg_views_per_message,
                channel_activity_level
            FROM analytics.dim_channels
            ORDER BY channel_name
        """
        try:
            return self.db.execute_query(query)
        except Exception as e:
            logger.error(f"Error getting channel list: {e}")
            raise

# Global CRUD instance
analytics_crud = AnalyticsCRUD()
