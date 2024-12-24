{{ config(
    materialized='incremental',
    unique_key='src_ordernumber'
) }}

WITH ranked_data AS (
    SELECT
        sd.ordernumber as src_ordernumber,
        ed.dw_customer_id,
        sd.orderdate,
        sd.requireddate,
        sd.shippeddate,
        sd.cancelleddate,
        sd.status,
        sd.customernumber as src_customernumber,
        ROW_NUMBER() OVER (ORDER BY sd.ordernumber) + COALESCE(MAX(ed.dw_order_id) OVER (), 0) AS dw_order_id,  -- Adding dw_order_id
        CASE
            WHEN sd.ordernumber IS NOT NULL AND ed.src_ordernumber IS NULL THEN sd.create_timestamp
            ELSE ed.src_create_timestamp
        END AS src_create_timestamp,
        COALESCE(sd.update_timestamp, ed.src_update_timestamp) AS src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        CASE
            WHEN sd.ordernumber IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE ed.dw_update_timestamp
        END AS dw_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp
    FROM
        {{ source('devstage', 'orders') }} sd  -- Referencing devstage.orders
    LEFT JOIN {{ source('devdw', 'orders') }} ed ON sd.ordernumber = ed.src_ordernumber  -- Referencing devdw.orders
    CROSS JOIN {{ source('metadata', 'batch_control') }} em  -- Referencing metadata.batch_control
)

SELECT 
    src_ordernumber,
    dw_customer_id,
    dw_order_id,  -- Added dw_order_id to the SELECT statement
    orderdate,
    requireddate,
    shippeddate,
    cancelleddate,
    status,
    src_customernumber,
    src_create_timestamp,
    src_update_timestamp,
    dw_update_timestamp,
    dw_create_timestamp,
    etl_batch_no,
    etl_batch_date
FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.src_ordernumber IS NOT NULL  -- Only process new or updated rows
{% endif %}
