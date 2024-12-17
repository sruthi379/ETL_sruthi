{{ config(
    materialized='incremental',
    unique_key='dw_product_id'  
) }}

WITH 
    batch_data AS (
        SELECT etl_batch_date, etl_batch_no 
        FROM {{source("metadata", "batch_control")}}
    ),
    update_data AS (
        SELECT 
            hist.dw_product_id,
            hist.msrp,
            hist.effective_from_date,
            0 AS dw_active_record_ind,
            DATEADD(day, -1, b.etl_batch_date) AS effective_to_date,
            hist.dw_create_timestamp,
            current_timestamp AS dw_update_timestamp,
            hist.create_etl_batch_date,
            hist.create_etl_batch_no,
            b.etl_batch_no AS update_etl_batch_no,
            b.etl_batch_date AS update_etl_batch_date
        FROM {{source("devdw", "product_history")}} hist
        JOIN {{ref("products")}} prod 
            ON hist.dw_product_id = prod.dw_product_id
        CROSS JOIN batch_data b
        WHERE 
            hist.msrp != prod.msrp 
            AND hist.dw_active_record_ind = 1
    ),
    insert_data AS (
        SELECT 
            prod.dw_product_id,
            prod.msrp,
            b.etl_batch_date AS effective_from_date,
            1 AS dw_active_record_ind,
            NULL AS effective_to_date,
            current_timestamp AS dw_create_timestamp,
            current_timestamp AS dw_update_timestamp,
            b.etl_batch_date AS create_etl_batch_date,
            b.etl_batch_no AS create_etl_batch_no,
            NULL AS update_etl_batch_no,
            NULL AS update_etl_batch_date
        FROM JOIN {{ref("products")}} prod
        LEFT JOIN {{source("devdw", "product_history")}} hist
            ON prod.dw_product_id = hist.dw_product_id
            AND hist.dw_active_record_ind = 1
        CROSS JOIN batch_data b
        WHERE hist.dw_product_id IS NULL
    ),
    merged_data AS (
        SELECT * FROM update_data
        UNION ALL
        SELECT * FROM insert_data
    )
SELECT * 
FROM merged_data
