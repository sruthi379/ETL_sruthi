-- models/monthly_product_summary.sql
{{ config(
    materialized='incremental',
    unique_key=['dw_product_id' ,'start_of_the_month_date']
) }}
WITH batch_info AS (
    SELECT 
        etl_batch_no,
        etl_batch_date
    FROM {{source("metadata", "batch_control")}}
),

existing_updates AS (
    SELECT
        DATE_TRUNC('month', src_data.summary_date) AS start_of_the_month_date,
        src_data.dw_product_id,
        GREATEST(tar_data.customer_apd, src_data.customer_apd) AS customer_apd,
        LEAST(tar_data.customer_apd + src_data.customer_apd, 1) AS customer_apm,
        tar_data.product_cost_amount + src_data.product_cost_amount AS product_cost_amount,
        tar_data.product_mrp_amount + src_data.product_mrp_amount AS product_mrp_amount,
        tar_data.cancelled_product_qty + src_data.cancelled_product_qty AS cancelled_product_qty,
        tar_data.cancelled_cost_amount + src_data.cancelled_cost_amount AS cancelled_cost_amount,
        tar_data.cancelled_mrp_amount + src_data.cancelled_mrp_amount AS cancelled_mrp_amount,
        GREATEST(tar_data.cancelled_order_apd, src_data.cancelled_order_apd) AS cancelled_order_apd,
        LEAST(tar_data.cancelled_order_apd + src_data.cancelled_order_apd, 1) AS cancelled_order_apm,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        batch_info.etl_batch_no AS etl_batch_no,
        batch_info.etl_batch_date AS etl_batch_date
    FROM {{ref("daily_product_summary")}} src_data
    JOIN {{source("devdw", "monthly_product_summary")}} tar_data
        ON tar_data.dw_product_id = src_data.dw_product_id
        AND DATE_TRUNC('month', src_data.summary_date) = tar_data.start_of_the_month_date
    JOIN batch_info
        ON src_data.summary_date >= batch_info.etl_batch_date
),

new_entries AS (
    SELECT
        DATE_TRUNC('month', src_data.summary_date) AS start_of_the_month_date,
        src_data.dw_product_id,
        MAX(src_data.customer_apd) AS customer_apd,
        CASE 
            WHEN MAX(src_data.customer_apd) = 1 THEN 1
            ELSE 0
        END AS customer_apm,
        SUM(src_data.product_cost_amount) AS product_cost_amount,
        SUM(src_data.product_mrp_amount) AS product_mrp_amount,
        SUM(src_data.cancelled_product_qty) AS cancelled_product_qty,
        SUM(src_data.cancelled_cost_amount) AS cancelled_cost_amount,
        SUM(src_data.cancelled_mrp_amount) AS cancelled_mrp_amount,
        MAX(src_data.cancelled_order_apd) AS cancelled_order_apd,
        CASE 
            WHEN MAX(src_data.cancelled_order_apd) = 1 THEN 1
            ELSE 0
        END AS cancelled_order_apm,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        batch_info.etl_batch_no AS etl_batch_no,
        batch_info.etl_batch_date AS etl_batch_date
    FROM {{ref("daily_product_summary")}} src_data
    LEFT JOIN {{source("devdw", "monthly_product_summary")}} tar_data
        ON tar_data.dw_product_id = src_data.dw_product_id
        AND DATE_TRUNC('month', src_data.summary_date) = tar_data.start_of_the_month_date
    JOIN batch_info
        ON TRUE
    WHERE tar_data.dw_product_id IS NULL
        AND tar_data.start_of_the_month_date IS NULL
    GROUP BY 
        DATE_TRUNC('month', src_data.summary_date), 
        src_data.dw_product_id,
        batch_info.etl_batch_no,
        batch_info.etl_batch_date
)

-- Combine both update and insert results without UNION
SELECT
    start_of_the_month_date,
    dw_product_id,
    customer_apd,
    customer_apm,
    product_cost_amount,
    product_mrp_amount,
    cancelled_product_qty,
    cancelled_cost_amount,
    cancelled_mrp_amount,
    cancelled_order_apd,
    cancelled_order_apm,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
FROM existing_updates

WHERE TRUE -- Process only incremental data in updates

UNION ALL -- Ensure same column structure for both parts
SELECT
    start_of_the_month_date,
    dw_product_id,
    customer_apd,
    customer_apm,
    product_cost_amount,
    product_mrp_amount,
    cancelled_product_qty,
    cancelled_cost_amount,
    cancelled_mrp_amount,
    cancelled_order_apd,
    cancelled_order_apm,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
FROM new_entries
