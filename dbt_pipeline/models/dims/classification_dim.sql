WITH src_attractions AS (SELECT * FROM {{ ref('src_events_attractions') }}),
src_events AS (SELECT * FROM {{ ref('src_events') }})



SELECT
DISTINCT
{{ dbt_utils.generate_surrogate_key(['SEGMENT_ID', 'GENRE_ID','SUB_GENRE_ID']) }}    AS CLASS_KEY,
    SEGMENT_ID,
    SEGMENT,
    GENRE_ID,
    GENRE,
    SUB_GENRE_ID,
    SUB_GENRE
FROM src_events

UNION

SELECT 
DISTINCT
{{ dbt_utils.generate_surrogate_key(['SEGMENT_ID', 'GENRE_ID','SUB_GENRE_ID']) }}    AS CLASS_KEY,
    SEGMENT_ID,
    SEGMENT,
    GENRE_ID, 
    GENRE,
    SUB_GENRE_ID,
    SUB_GENRE,
FROM src_attractions

