{{ config(materialized = 'view') }} WITH image_detections AS (
    SELECT id,
        image_path,
        image_hash,
        message_id,
        channel_name,
        detected_object_class,
        confidence_score,
        bbox_x1,
        bbox_y1,
        bbox_x2,
        bbox_y2,
        detection_date,
        model_version,
        -- Calculate bounding box area
        (bbox_x2 - bbox_x1) * (bbox_y2 - bbox_y1) AS bbox_area,
        -- Calculate bounding box center
        (bbox_x1 + bbox_x2) / 2 AS bbox_center_x,
        (bbox_y1 + bbox_y2) / 2 AS bbox_center_y,
        -- Categorize confidence levels
        CASE
            WHEN confidence_score >= 0.8 THEN 'high'
            WHEN confidence_score >= 0.5 THEN 'medium'
            ELSE 'low'
        END AS confidence_level
    FROM {{ source('raw', 'image_detections') }}
    WHERE message_id IS NOT NULL
)
SELECT *
FROM image_detections