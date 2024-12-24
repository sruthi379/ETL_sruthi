{{ config(
    materialized='incremental',
    unique_key='src_productcode'
) }}

WITH ranked_data AS (
    SELECT
        sd.productcode AS src_productcode,
        sd.productname,
        sd.productline,
        sd.productscale,
        sd.productvendor,
        sd.quantityinstock,
        sd.buyprice,
        sd.msrp,
        ROW_NUMBER() OVER (ORDER BY sd.productcode) 
            + COALESCE(MAX(ed.dw_product_id) OVER (), 0) AS dw_product_id,
        pl.dw_product_line_id,
        CASE
            WHEN sd.productcode IS NOT NULL AND ed.src_productcode IS NULL THEN sd.create_timestamp
            ELSE ed.src_create_timestamp
        END AS src_create_timestamp,
        COALESCE(sd.update_timestamp, ed.src_update_timestamp) AS src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        CASE
            WHEN sd.productcode IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE ed.dw_update_timestamp
        END AS dw_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp
    FROM
        {{ source('devstage', 'products') }} sd  -- Referencing devstage.products
    LEFT JOIN {{ source('devdw', 'products') }} ed ON sd.productcode = ed.src_productcode  -- Referencing devdw.products
    LEFT JOIN {{ ref('productlines') }} pl ON sd.productline = pl.productline  -- Referencing devdw.productlines
    CROSS JOIN {{ source('metadata', 'batch_control') }} em  -- Referencing metadata.batch_control
)

SELECT 
    src_productcode,
    productname,
    productline,
    productscale,
    productvendor,
    quantityinstock,
    buyprice,
    msrp,
    dw_product_id,
    dw_product_line_id,
    src_create_timestamp,
    src_update_timestamp,
    dw_update_timestamp,
    dw_create_timestamp,
    etl_batch_no,
    etl_batch_date
FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.src_productcode IS NOT NULL  -- Only process new or updated rows
{% endif %}
