with
    batch_data as (
        select etl_batch_date, etl_batch_no 
        from metadata.batch_control
    ),
    update_data as (
        select 
            hist.dw_product_id,
            hist.msrp,
            hist.effective_from_date,
            0 as dw_active_record_ind,
            DATEADD(day, -1, b.etl_batch_date) as effective_to_date,
            hist.dw_create_timestamp,
            current_timestamp as dw_update_timestamp,
            hist.create_etl_batch_date,
            hist.create_etl_batch_no,
            b.etl_batch_no as update_etl_batch_no,
            b.etl_batch_date as update_etl_batch_date
        from "dev"."devdw"."product_history" hist
        join "dev"."devdw"."products" prod 
            on hist.dw_product_id = prod.dw_product_id
        cross join batch_data b
        where 
            hist.msrp != prod.msrp 
            and hist.dw_active_record_ind = 1
    ),
    insert_data as (
        select 
            prod.dw_product_id,
            prod.msrp,
            b.etl_batch_date as effective_from_date,
            1 as dw_active_record_ind,
            null as effective_to_date,
            current_timestamp as dw_create_timestamp,
            current_timestamp as dw_update_timestamp,
            b.etl_batch_date as create_etl_batch_date,
            b.etl_batch_no as create_etl_batch_no,
            null as update_etl_batch_no,
            null as update_etl_batch_date
        from "dev"."devdw"."products" prod
        left join "dev"."devdw"."product_history" hist
            on prod.dw_product_id = hist.dw_product_id
            and hist.dw_active_record_ind = 1
        cross join batch_data b
        where hist.dw_product_id is null
    ),
    merged_data as (
        select * from update_data
        union all
        select * from insert_data
    )
select * from merged_data
