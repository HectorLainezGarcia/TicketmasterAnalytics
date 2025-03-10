WITH src_events AS (SELECT * FROM {{ ref('src_events') }}),
src_promoter    AS  ( SELECT EVENT_DLT_ID, COUNT(PROMOTER_ID) AS PROMOTER_COUNT FROM {{ ref('src_promoter') }} GROUP BY EVENT_DLT_ID),
src_attractions AS  ( SELECT EVENT_DLT_ID, COUNT(ATTRACTION_ID) AS ATTRACTION_COUNT FROM {{ ref('src_events_attractions') }} GROUP BY EVENT_DLT_ID)




SELECT
DISTINCT 
{{ dbt_utils.generate_surrogate_key(['EVENT_ID', 'EVENT_STATUS','EVENT_START_DATE','EVENT_END_DATE']) }}    AS EVENT_KEY,
{{ dbt_utils.generate_surrogate_key(['SEGMENT_ID', 'GENRE_ID','SUB_GENRE_ID']) }}                           AS CLASS_KEY,
EVENT_ID,
EVENT_NAME,
EVENT_STATUS,  
COALESCE(VENUE_ID, 'NA')                                                                                    AS VENUE_ID,
COALESCE(VENUE_NAME,'NA')                                                                                   AS VENUE_NAME,
COALESCE(EVENT_LEGACY_ID, 'NA')                                                                             AS LEGACY_ID,
COALESCE(CLASSIFICATION_TYPE_ID,'NA')                                                                       AS CLASSIFICATION_TYPE_ID,
COALESCE(CLASSIFICATION_TYPE, 'NA')                                                                         AS CLASSIFICATION_TYPE,
COALESCE(CLASSIFICATION_SUB_TYPE_ID, 'NA')                                                                  AS CLASSIFICATION_SUB_TYPE_ID,
COALESCE(CLASSIFICATION_SUB_TYPE, 'NA')                                                                     AS CLASSIFICATION_SUB_TYPE,
COALESCE(SEGMENT_ID, 'NA')                                                                                  AS SEGMENT_ID,
COALESCE(SEGMENT, 'NA')                                                                                     AS SEGMENT,
COALESCE(GENRE_ID, 'NA')                                                                                    AS GENRE_ID,
COALESCE(GENRE,'NA')                                                                                        AS GENRE,
COALESCE(SUB_GENRE_ID,'NA')                                                                                 AS SUB_GENRE_ID,
COALESCE(SUB_GENRE,'NA')                                                                                    AS SUB_GENRE,
COALESCE(SOURCE,'NA')                                                                                       AS SOURCE,
COALESCE(EVENT_URL, 'NA')                                                                                   AS EVENT_URL,
IS_HOT, 
EVENT_START_DATE, 
EVENT_END_DATE,
ONSALE_START_DATE,
ONSALE_END_DATE,
MIN_PRICE,
MAX_PRICE,
(MAX_PRICE+MIN_PRICE)/2                             AS AVG_PRICE,
MIN_PRICES_FEES,
MAX_PRICES_FEES,
(MAX_PRICES_FEES+MIN_PRICES_FEES)/2                 AS AVG_PRICE_FEES,
COALESCE(CURRENCY, 'NA')                            AS CURRENCY,
COALESCE(C.ATTRACTION_COUNT,0)                      AS ATTRACTION_COUNT,
DATEDIFF(DAYS,EVENT_START_DATE,EVENT_END_DATE)      AS EVENT_DAYS_DURATION,

FROM src_events AS A 
LEFT JOIN src_promoter B ON A.DLT_ID = B.EVENT_DLT_ID 
LEFT JOIN src_attractions C on A.DLT_ID = C.EVENT_DLT_ID
