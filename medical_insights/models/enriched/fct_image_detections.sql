{{ config(
    materialized = 'table', schema='enriched'
) }}

SELECT
    detections.message_id,
    detections.detected_object AS object_class,
    detections.confidence_score,
    detections.bbox
FROM
    {{ source('enriched', 'fct_image_detections') }} AS detections
LEFT JOIN
    {{ ref('fct_messages') }} AS messages
    ON detections.message_id = messages.message_id
    