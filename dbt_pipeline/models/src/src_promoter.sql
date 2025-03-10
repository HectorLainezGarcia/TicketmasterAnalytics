WITH stg_promotor_hist AS ( SELECT * FROM {{ source('landing_historical', 'events_promotors') }})

SELECT 
    _DLT_PARENT_ID          AS EVENT_DLT_ID,
    PROMOTER__ID            AS PROMOTER_ID,
    PROMOTER__NAME          AS PROMOTER_NAME,
    PROMOTER__DESCRIPTION   AS PROMOTER_DESCRIPTION
 FROM stg_promotor_hist