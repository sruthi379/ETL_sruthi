

{{ config(
    materialized='incremental',
    unique_key=['src_ordernumber', 'src_productcode']
) }}

-- Fetch the latest batch metadata
WITH batch_control AS (
    SELECT 
        etl_batch_no, 
        etl_batch_date
    FROM {{ source('metadata', 'batch_control') }}
),

-- Fetch staging data for orderdetails
staging_orderdetails AS (
    SELECT
        ordernumber AS src_ordernumber,
        productcode AS src_productcode,
        quantityordered,
        priceeach,
        orderlinenumber,
        update_timestamp AS src_update_timestamp,
        create_timestamp AS src_create_timestamp
    FROM {{ source('devstage', 'orderdetails') }}
),

-- Fetch existing data in target for incremental load (only records that need updating or inserting)
existing_orderdetails AS (
    SELECT 
        dw_orderdetail_id,  
        src_ordernumber, 
        src_productcode, 
        dw_order_id, 
        dw_product_id
    FROM {{ this }}
),

-- Combine staging and existing data to determine which records need to be inserted or updated
final_data AS (
    SELECT
        st.src_ordernumber,
        st.src_productcode,
        st.quantityordered,
        st.priceeach,
        st.orderlinenumber,
        st.src_create_timestamp,
        st.src_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        bc.etl_batch_no,
        bc.etl_batch_date,
        -- Add foreign keys for dw_order_id and dw_product_id
        o.dw_order_id,
        p.dw_product_id,
        -- Calculate the dw_orderdetail_id based on the max existing ID + row number
        ROW_NUMBER() OVER() + COALESCE(MAX(dw.dw_orderdetail_id) OVER(), 0) AS dw_orderdetail_id
    FROM staging_orderdetails AS st
    CROSS JOIN batch_control AS bc
    LEFT JOIN existing_orderdetails AS dw
        ON st.src_ordernumber = dw.src_ordernumber
        AND st.src_productcode = dw.src_productcode
    LEFT JOIN {{ source('devdw', 'orders') }} o ON st.src_ordernumber = o.src_ordernumber
    LEFT JOIN {{ source('devdw', 'products') }} p ON st.src_productcode = p.src_productcode
    WHERE dw.src_ordernumber IS NULL
)

-- Insert or update records in the target table
SELECT
    dw_orderdetail_id,
    src_ordernumber,
    src_productcode,
    quantityordered,
    priceeach,
    orderlinenumber,
    src_create_timestamp,
    src_update_timestamp,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date,
    dw_order_id,
    dw_product_id
FROM final_data

{% if is_incremental() %}
WHERE
    final_data.src_ordernumber IS NOT NULL  -- Only process new or updated rows
{% endif %}
