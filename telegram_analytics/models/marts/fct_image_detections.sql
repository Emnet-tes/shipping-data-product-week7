{{ config(materialized = 'table') }} WITH image_detections AS (
    SELECT id AS detection_id,
        message_id,
        MD5(channel_name) as channel_key,
        detected_object_class,
        confidence_score,
        confidence_level,
        bbox_area,
        bbox_center_x,
        bbox_center_y,
        detection_date,
        model_version,
        image_hash,
        -- Create detection score based on confidence and object importance
        CASE
            WHEN detected_object_class IN ('person', 'face') THEN confidence_score * 1.2
            WHEN detected_object_class IN ('bottle', 'cup', 'cell phone') THEN confidence_score * 1.1
            ELSE confidence_score
        END AS detection_score
    FROM {{ ref('stg_image_detections') }}
),
message_context AS (
    SELECT message_id,
        channel_key,
        date_key,
        message_date,
        has_media,
        engagement_score
    FROM {{ ref('fct_messages') }}
)
SELECT i.detection_id,
    i.message_id,
    i.channel_key,
    m.date_key,
    i.detected_object_class,
    i.confidence_score,
    i.confidence_level,
    i.bbox_area,
    i.bbox_center_x,
    i.bbox_center_y,
    i.detection_score,
    i.detection_date,
    i.model_version,
    i.image_hash,
    m.message_date,
    m.engagement_score
FROM image_detections i
    LEFT JOIN message_context m ON i.message_id = m.message_id