{{ config(
    materialized='incremental',
    unique_key='officecode'
) }}

WITH ranked_data AS (
    SELECT
        sd.officecode,
        sd.city,
        sd.phone,
        sd.addressline1,
        sd.addressline2,
        sd.state,
        sd.country,
        sd.postalcode,
        sd.territory,
        ROW_NUMBER() OVER (ORDER BY sd.officecode) 
            + COALESCE(MAX(ed.dw_office_id) OVER (), 0) AS dw_office_id,
        CASE
            WHEN sd.officecode IS NOT NULL AND ed.officecode IS NULL THEN sd.create_timestamp
            ELSE ed.src_create_timestamp
        END AS src_create_timestamp,
        COALESCE(sd.update_timestamp, ed.src_update_timestamp) AS src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        CASE
            WHEN sd.officecode IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE ed.dw_update_timestamp
        END AS dw_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp
    FROM
        devstage.offices sd --{{ source('devstage', 'offices') }} sd  -- Referencing devstage.offices
    LEFT JOIN devdw.offices ed ON sd.officecode = ed.officecode --{{ source('devdw', 'offices') }} ed ON sd.officecode = ed.officecode  -- Referencing devdw.offices
    CROSS JOIN metadata.batch_control em --{{ source('metadata', 'batch_control') }} em  -- Referencing metadata.batch_control
)

SELECT 
    dw_office_id,
    officecode,
    city,
    phone,
    addressline1,
    addressline2,
    state,
    country,
    postalcode,
    territory,
    src_create_timestamp,
    src_update_timestamp,
    dw_update_timestamp,
    dw_create_timestamp,
    etl_batch_no,
    etl_batch_date
FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.officecode IS NOT NULL  -- Only process new or updated rows
{% endif %}
