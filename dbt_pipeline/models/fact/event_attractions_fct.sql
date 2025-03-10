WITH src_events AS (SELECT * FROM {{ ref('src_events') }}),
src_promoter    AS  ( SELECT EVENT_DLT_ID, COUNT(PROMOTER_ID) AS PROMOTER_COUNT FROM {{ ref('src_promoter') }} GROUP BY EVENT_DLT_ID),
src_attractions AS  ( SELECT * FROM {{ ref('src_events_attractions') }})

SELECT  
{{ dbt_utils.generate_surrogate_key(['A.EVENT_ID', 'A.EVENT_STATUS','A.EVENT_START_DATE','A.EVENT_END_DATE','A.CLASSIFICATION_TYPE_ID']) }}        AS EVENT_KEY,
{{ dbt_utils.generate_surrogate_key(['B.ATTRACTION_ID', 'B.ATTRACTION_NAME']) }}                                AS ATTRACTION_KEY,
{{ dbt_utils.generate_surrogate_key(['A.SEGMENT_ID', 'A.GENRE_ID','A.SUB_GENRE_ID']) }}                         AS EVENT_CLASS_KEY,
{{ dbt_utils.generate_surrogate_key(['B.SEGMENT_ID', 'B.GENRE_ID','B.SUB_GENRE_ID']) }}                         AS ATTRACTION_CLASS_KEY,
{{ dbt_utils.generate_surrogate_key(['A.VENUE_ID', 'A.VENUE_NAME']) }}                                          AS VENUE_KEY,
A.EVENT_NAME                                                                                                    AS EVENT_NAME,
B.ATTRACTION_NAME                                                                                               AS ATTRACTION_NAME,
CASE WHEN C.PROMOTER_COUNT > 0 
    THEN 'Yes'
    ELSE 'No' 
    END                                                                                                         AS IS_PROMOTED,
COALESCE(C.PROMOTER_COUNT,0)                                                                                    AS PROMOTER_COUNT,
CASE WHEN 
DATEDIFF(DAYS,EVENT_START_DATE,EVENT_END_DATE)>0 
    THEN 'Yes'
    ELSE 'No'
    END                                                                                                         AS MULTIPLE_DAYS


FROM src_events AS A 
LEFT JOIN src_attractions B on A.DLT_ID = B.EVENT_DLT_ID
LEFT JOIN src_promoter C ON A.DLT_ID = C.EVENT_DLT_ID 
