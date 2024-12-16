{{ config(
    materialized='incremental',
    unique_key='dw_customer_id'
) }}


WITH batch_info AS (
    -- Fetch the latest ETL batch information
    SELECT 
        etl_batch_no AS etl_batch_no,
        etl_batch_date AS etl_batch_date
    FROM metadata.batch_control
),

updated_records AS (
    -- Identify records that need to be updated
    SELECT 
        hist.dw_customer_id,
        hist.creditLimit,
        NULL AS effective_from_date, -- Placeholder for UNION compatibility
        DATEADD(DAY, -1, batch_info.etl_batch_date) AS effective_to_date,
        0 AS dw_active_record_ind,
        hist.dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        NULL AS create_etl_batch_no, -- Placeholder for UNION compatibility
        NULL AS create_etl_batch_date, -- Placeholder for UNION compatibility
        batch_info.etl_batch_no AS update_etl_batch_no,
        batch_info.etl_batch_date AS update_etl_batch_date
    FROM devdw.customer_history hist
    JOIN devdw.Customers src
      ON hist.dw_customer_id = src.dw_customer_id
    JOIN batch_info
      ON 1 = 1
    WHERE hist.dw_active_record_ind = 1
      AND src.creditLimit <> hist.creditLimit
),

insert_new_records AS (
    -- Identify new records that need to be inserted
    SELECT 
        src.dw_customer_id,
        src.creditLimit,
        batch_info.etl_batch_date AS effective_from_date,
        NULL AS effective_to_date, -- Placeholder for UNION compatibility
        1 AS dw_active_record_ind,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        batch_info.etl_batch_no AS create_etl_batch_no,
        batch_info.etl_batch_date AS create_etl_batch_date,
        NULL AS update_etl_batch_no, -- Placeholder for UNION compatibility
        NULL AS update_etl_batch_date -- Placeholder for UNION compatibility
    FROM devdw.Customers src
    LEFT JOIN devdw.customer_history hist
      ON src.dw_customer_id = hist.dw_customer_id
      AND hist.dw_active_record_ind = 1
    JOIN batch_info
      ON 1 = 1
    WHERE hist.dw_customer_id IS NULL
)

-- Combine updated and new records for insertion into customer_history
SELECT 
    dw_customer_id,
    creditLimit,
    effective_from_date,
    effective_to_date,
    dw_active_record_ind,
    dw_create_timestamp,
    dw_update_timestamp,
    create_etl_batch_no,
    create_etl_batch_date,
    update_etl_batch_no,
    update_etl_batch_date
FROM updated_records

UNION ALL

SELECT 
    dw_customer_id,
    creditLimit,
    effective_from_date,
    effective_to_date,
    dw_active_record_ind,
    dw_create_timestamp,
    dw_update_timestamp,
    create_etl_batch_no,
    create_etl_batch_date,
    update_etl_batch_no,
    update_etl_batch_date
FROM insert_new_records
