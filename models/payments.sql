-- File: models/payments.sql

{{
    config(
        materialized='incremental',
        unique_key='checknumber' 
    )
}}

WITH batch_control AS (
    SELECT
        etl_batch_no,
        etl_batch_date
    FROM {{ source('metadata', 'batch_control') }}
    ORDER BY etl_batch_date DESC
    LIMIT 1
),
new_payments AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY payments_src.CHECKNUMBER) + 
        (SELECT COALESCE(MAX(DW_PAYMENT_ID), 0) 
         FROM {{ source('devdw', 'payments') }}) AS DW_PAYMENT_ID,  -- Incrementing DW_PAYMENT_ID
        payments_src.CUSTOMERNUMBER AS SRC_CUSTOMERNUMBER,
        customers_dw.DW_CUSTOMER_ID AS DW_CUSTOMER_ID,
        payments_src.CHECKNUMBER,
        payments_src.PAYMENTDATE,
        payments_src.AMOUNT,
        payments_src.CREATE_TIMESTAMP AS SRC_CREATE_TIMESTAMP,
        payments_src.UPDATE_TIMESTAMP AS SRC_UPDATE_TIMESTAMP,
        CURRENT_TIMESTAMP AS DW_CREATE_TIMESTAMP,
        CURRENT_TIMESTAMP AS DW_UPDATE_TIMESTAMP,
        batch_control.etl_batch_no AS ETL_BATCH_NO,
        batch_control.etl_batch_date AS ETL_BATCH_DATE
    FROM {{ source('devstage', 'payments') }} AS payments_src
    CROSS JOIN batch_control  -- Ensures batch details are included
    LEFT JOIN {{ source('devdw', 'payments') }} AS payments_dw
        ON payments_src.CHECKNUMBER = payments_dw.CHECKNUMBER
    LEFT JOIN {{ ref('customers') }} AS customers_dw
        ON payments_src.CUSTOMERNUMBER = customers_dw.SRC_CUSTOMERNUMBER
    WHERE payments_dw.CHECKNUMBER IS NULL
)

-- Insert new records into the incremental model
SELECT *
FROM new_payments
