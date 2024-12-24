{{ config(
    materialized='incremental',
    unique_key=['dw_customer_id', 'start_of_the_month_date']
) }}

WITH 
    batch_info AS (
        SELECT 
            etl_batch_date, 
            etl_batch_no 
        FROM {{ source('metadata', 'batch_control') }}
    ),
    aggregated_data AS (
        SELECT
            DATE_TRUNC('month', summary_date) AS start_of_the_month_date,
            dw_customer_id,
            SUM(order_count) AS order_count,
            MAX(order_apd) AS order_apd,
            MAX(order_apd) AS order_apm,
            SUM(order_cost_amount) AS order_cost_amount,
            SUM(cancelled_order_count) AS cancelled_order_count,
            SUM(cancelled_order_amount) AS cancelled_order_amount,
            MAX(cancelled_order_apd) AS cancelled_order_apd,
            MAX(cancelled_order_apd) AS cancelled_order_apm,
            SUM(shipped_order_count) AS shipped_order_count,
            SUM(shipped_order_amount) AS shipped_order_amount,
            MAX(shipped_order_apd) AS shipped_order_apd,
            MAX(shipped_order_apd) AS shipped_order_apm,
            MAX(payment_apd) AS payment_apd,
            MAX(payment_apd) AS payment_apm,
            SUM(payment_amount) AS payment_amount,
            SUM(products_ordered_qty) AS products_ordered_qty,
            SUM(products_items_qty) AS products_items_qty,
            SUM(order_mrp_amount) AS order_mrp_amount,
            MAX(new_customer_apd) AS new_customer_apd,
            MAX(new_customer_apd) AS new_customer_apm,
            0 AS new_customer_paid_apd,
            0 AS new_customer_paid_apm
        FROM {{ ref( 'daily_customer_summary') }} src_data
        CROSS JOIN batch_info b
        WHERE DATE_TRUNC('month', summary_date) >= DATE_TRUNC('month', b.etl_batch_date)
        GROUP BY DATE_TRUNC('month', summary_date), dw_customer_id
    )

SELECT
    agg_data.start_of_the_month_date,
    agg_data.dw_customer_id,
    agg_data.order_count,
    agg_data.order_apd,
    agg_data.order_apm,
    agg_data.order_cost_amount,
    agg_data.cancelled_order_count,
    agg_data.cancelled_order_amount,
    agg_data.cancelled_order_apd,
    agg_data.cancelled_order_apm,
    agg_data.shipped_order_count,
    agg_data.shipped_order_amount,
    agg_data.shipped_order_apd,
    agg_data.shipped_order_apm,
    agg_data.payment_apd,
    agg_data.payment_apm,
    agg_data.payment_amount,
    agg_data.products_ordered_qty,
    agg_data.products_items_qty,
    agg_data.order_mrp_amount,
    agg_data.new_customer_apd,
    agg_data.new_customer_apm,
    agg_data.new_customer_paid_apd,
    agg_data.new_customer_paid_apm,
    CURRENT_TIMESTAMP AS dw_create_timestamp,
    CURRENT_TIMESTAMP AS dw_update_timestamp,
    b.etl_batch_no,
    b.etl_batch_date
FROM aggregated_data agg_data
CROSS JOIN batch_info b
