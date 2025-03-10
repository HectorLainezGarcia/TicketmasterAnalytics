WITH src_attractions AS (SELECT * FROM {{ ref('src_events_attractions') }})

SELECT 
DISTINCT
    {{ dbt_utils.generate_surrogate_key(['ATTRACTION_ID', 'ATTRACTION_NAME']) }}         AS ATTRACTION_KEY,
    ATTRACTION_ID, 
    {{ dbt_utils.generate_surrogate_key(['SEGMENT_ID', 'GENRE_ID','SUB_GENRE_ID']) }}    AS CLASS_KEY,
    ATTRACTION_LEGACY_ID,
    ATTRACTION_NAME,
    ATTRACTION_URL,
    SEGMENT_ID,
    SEGMENT,
    GENRE_ID, 
    GENRE,
    SUB_GENRE_ID,
    SUB_GENRE,
    CLASSIFICATION_TYPE_ID,
    CLASSIFICATION_TYPE,
    CLASSIFICATION_SUB_TYPE_ID,
    CLASSIFICATION_SUB_TYPE
FROM src_attractions