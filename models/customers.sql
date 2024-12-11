{{ config(
    materialized='incremental',
    unique_key='src_customernumber'
) }}

WITH ranked_data AS (
    SELECT
        sd.customernumber AS src_customernumber,
        sd.customername,
        sd.contactlastname,
        sd.contactfirstname,
        sd.phone,
        sd.addressline1,
        sd.addressline2,
        sd.city,
        sd.state,
        sd.postalcode,
        sd.country,
        sd.salesrepemployeenumber,
        sd.creditlimit,
        ROW_NUMBER() OVER (ORDER BY sd.customernumber) 
            + COALESCE(MAX(ed.dw_customer_id) OVER (), 0) AS dw_customer_id,
        CASE
            WHEN sd.customernumber IS NOT NULL AND ed.src_customernumber IS NULL THEN sd.create_timestamp
            ELSE ed.src_create_timestamp
        END AS src_create_timestamp,
        COALESCE(sd.update_timestamp, ed.src_update_timestamp) AS src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        CASE
            WHEN sd.customernumber IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE ed.dw_update_timestamp
        END AS dw_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp
    FROM
        {{ source('devstage', 'customers') }} sd  -- Referencing devstage.customers
    LEFT JOIN {{ source('devdw', 'customers') }} ed ON sd.customernumber = ed.src_customernumber  -- Referencing devdw.customers
    CROSS JOIN {{ source('metadata', 'batch_control') }} em  -- Referencing metadata.batch_control
)

SELECT 
    dw_customer_id,
    src_customernumber,
    customername,
    contactlastname,
    contactfirstname,
    phone,
    addressline1,
    addressline2,
    city,
    state,
    postalcode,
    country,
    salesrepemployeenumber,
    creditlimit,
    src_create_timestamp,
    src_update_timestamp,
    dw_update_timestamp,
    dw_create_timestamp,
    etl_batch_no,
    etl_batch_date
FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.src_customernumber IS NOT NULL  -- Only process new or updated rows
{% endif %}
