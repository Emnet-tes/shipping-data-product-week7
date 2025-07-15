{{ config(materialized = 'table') }} WITH object_stats AS (
    SELECT detected_object_class,
        COUNT(*) as total_detections,
        COUNT(DISTINCT message_id) as messages_with_object,
        COUNT(DISTINCT channel_name) as channels_with_object,
        AVG(confidence_score) as avg_confidence,
        MIN(confidence_score) as min_confidence,
        MAX(confidence_score) as max_confidence,
        AVG(bbox_area) as avg_bbox_area,
        -- Calculate object importance score
        COUNT(*) * AVG(confidence_score) as importance_score
    FROM {{ ref('stg_image_detections') }}
    GROUP BY detected_object_class
),
object_categories AS (
    SELECT detected_object_class,
        CASE
            WHEN detected_object_class IN ('person', 'face') THEN 'people'
            WHEN detected_object_class IN (
                'bottle',
                'cup',
                'wine glass',
                'bowl',
                'fork',
                'knife',
                'spoon'
            ) THEN 'food_drink'
            WHEN detected_object_class IN (
                'cell phone',
                'laptop',
                'mouse',
                'keyboard',
                'tv',
                'remote'
            ) THEN 'electronics'
            WHEN detected_object_class IN (
                'chair',
                'couch',
                'bed',
                'dining table',
                'toilet'
            ) THEN 'furniture'
            WHEN detected_object_class IN (
                'book',
                'clock',
                'vase',
                'scissors',
                'teddy bear'
            ) THEN 'household_items'
            WHEN detected_object_class IN (
                'car',
                'motorcycle',
                'airplane',
                'bus',
                'train',
                'truck',
                'boat'
            ) THEN 'vehicles'
            WHEN detected_object_class IN (
                'bicycle',
                'skateboard',
                'surfboard',
                'tennis racket',
                'baseball bat',
                'baseball glove',
                'skis',
                'snowboard',
                'sports ball',
                'kite',
                'frisbee'
            ) THEN 'sports'
            WHEN detected_object_class IN (
                'cat',
                'dog',
                'horse',
                'sheep',
                'cow',
                'elephant',
                'bear',
                'zebra',
                'giraffe',
                'bird'
            ) THEN 'animals'
            ELSE 'other'
        END as object_category
    FROM object_stats
)
SELECT MD5(s.detected_object_class) as object_key,
    s.detected_object_class,
    c.object_category,
    s.total_detections,
    s.messages_with_object,
    s.channels_with_object,
    s.avg_confidence,
    s.min_confidence,
    s.max_confidence,
    s.avg_bbox_area,
    s.importance_score,
    -- Categorize detection frequency
    CASE
        WHEN s.total_detections >= 50 THEN 'very_common'
        WHEN s.total_detections >= 20 THEN 'common'
        WHEN s.total_detections >= 5 THEN 'uncommon'
        ELSE 'rare'
    END as frequency_category
FROM object_stats s
    JOIN object_categories c ON s.detected_object_class = c.detected_object_class