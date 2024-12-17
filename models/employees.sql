
{{ config(
    materialized='incremental',
    unique_key='employeenumber'
) }}

WITH ranked_data AS (
    SELECT
        sd.employeenumber AS employeenumber,
        sd.lastname,
        sd.firstname,
        sd.extension,
        sd.email,
        sd.officecode,
        sd.reportsto,
        sd.jobtitle,
        ROW_NUMBER() OVER (ORDER BY sd.employeenumber) 
            + COALESCE(MAX(ed.dw_employee_id) OVER (), 0) AS dw_employee_id,
        CASE
            WHEN sd.employeenumber IS NOT NULL AND ed.employeenumber IS NULL THEN sd.create_timestamp
            ELSE ed.src_create_timestamp
        END AS src_create_timestamp,
        COALESCE(sd.update_timestamp, ed.src_update_timestamp) AS src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        CASE
            WHEN sd.employeenumber IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE ed.dw_update_timestamp
        END AS dw_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        o.dw_office_id,
        re.dw_employee_id AS dw_reporting_employee_id
    FROM
        {{ source('devstage', 'employees') }} sd  -- Referencing devstage.employees
    LEFT JOIN {{ source('devdw','employees') }} ed ON sd.employeenumber = ed.employeenumber  -- Referencing devdw.employees
    LEFT JOIN {{ ref('offices') }} o ON sd.officecode = o.officecode  -- Referencing devdw.offices
    LEFT JOIN {{ source('devdw', 'employees') }} re ON sd.reportsto = re.employeenumber  -- Referencing devdw.employees (for reporting)
    CROSS JOIN {{ source('metadata', 'batch_control') }} em  -- Referencing metadata.batch_control
)

SELECT 
    dw_employee_id,
    employeenumber,
    lastname,
    firstname,
    extension,
    email,
    officecode,
    reportsto,
    jobtitle,
    src_create_timestamp,
    src_update_timestamp,
    dw_update_timestamp,
    dw_create_timestamp,
    etl_batch_no,
    etl_batch_date,
    dw_office_id,
    dw_reporting_employee_id
FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.employeenumber IS NOT NULL  -- Only process new or updated rows
{% endif %}
