import os
import sys
import boto3
import psycopg2
sys.path.append(os.path.abspath(".."))
from config import redshift_username, redshift_password, redshift_dsn, bucket, tables
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS S3 and Redshift Setup
client = boto3.client('s3')

def get_etl_batch_info():
    """Fetch the ETL batch date and batch number from Redshift."""
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(
            dbname=redshift_dsn['dbname'],
            user=redshift_username,
            password=redshift_password,
            host=redshift_dsn['host'],
            port=redshift_dsn['port']
        )
        
        cursor = connection.cursor()
        etl_batch_info_query = "SELECT etl_batch_no, etl_batch_date FROM metadata.batch_control"
        cursor.execute(etl_batch_info_query)
        result = cursor.fetchone()
        
        if result is None:
            print("No ETL batch information found in the batch_control table.")
            return None, None
        
        # Return both the batch number and date
        return result[0], result[1]  # etl_batch_no, etl_batch_date
    except Exception as e:
        print(f"Error fetching batch information from Redshift: {e}")
        return None, None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def load_data_to_redshift(schema_name, etl_batch_date):
    """Load data from devstage to devdw and update timestamps in the DPS table."""
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(
            dbname=redshift_dsn['dbname'],
            user=redshift_username,
            password=redshift_password,
            host=redshift_dsn['host'],
            port=redshift_dsn['port']
        )
        cursor = connection.cursor()

        # Insert new records into the DPS table from the devstage schema

        insert_query = """
         INSERT INTO devdw.daily_product_summary (
            summary_date,
            dw_product_id,
            customer_apd,
            product_cost_amount,
            product_mrp_amount,
            cancelled_product_qty,
            cancelled_cost_amount,
            cancelled_mrp_amount,
            cancelled_order_apd,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            summary_date,
            dw_product_id,
            MAX(customer_apd) AS customer_apd,
            MAX(product_cost_amount) AS product_cost_amount,
            MAX(product_mrp_amount) AS product_mrp_amount,
            MAX(cancelled_product_qty) AS cancelled_product_qty,
            MAX(cancelled_cost_amount) AS cancelled_cost_amount,
            MAX(cancelled_mrp_amount) AS cancelled_mrp_amount,
            MAX(cancelled_order_apd) AS cancelled_order_apd,
            CURRENT_TIMESTAMP AS dw_create_timestamp,
            CURRENT_TIMESTAMP AS dw_update_timestamp,
            %s AS etl_batch_no,
            %s AS etl_batch_date
        FROM (
            SELECT 
                CAST(o.OrderDate AS DATE) AS summary_date,
                p.dw_product_id,
                1 AS customer_apd,
                SUM(od.priceEach * od.quantityOrdered) AS product_cost_amount,
                SUM(od.quantityordered * p.msrp) AS product_mrp_amount,
                0 AS cancelled_product_qty,
                0 AS cancelled_cost_amount,
                0 AS cancelled_mrp_amount,
                0 AS cancelled_order_apd
            FROM devdw.orders o
            JOIN devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
            JOIN devdw.products p ON od.src_productCode =p.src_productCode
            WHERE CAST(o.OrderDate AS DATE) >= %s
            GROUP BY CAST(o.OrderDate AS DATE), p.dw_product_id

            UNION ALL

            SELECT 
                CAST(o.cancelledDate AS DATE) AS summary_date,
                p.dw_product_id,
                0 AS customer_apd,
                0 AS product_cost_amount,
                0 AS product_mrp_amount,
                SUM(od.quantityOrdered) AS cancelled_product_qty,
                SUM(od.priceEach * od.quantityOrdered) AS cancelled_cost_amount,
                SUM(od.quantityordered * p.msrp) AS cancelled_mrp_amount,
                1 AS cancelled_order_apd
            FROM devdw.orders o
            JOIN devdw.orderdetails od ON o.dw_order_id = od.dw_order_id
            JOIN devdw.products p ON od.src_productCode = p.src_productCode
            WHERE CAST(o.cancelledDate AS DATE) >= %s
              AND o.status = 'Cancelled'
            GROUP BY CAST(o.cancelledDate AS DATE), p.dw_product_id
        ) x
        GROUP BY summary_date, dw_product_id;

        """
        
        # Execute the query with parameters
        cursor.execute(insert_query, (etl_batch_no, etl_batch_date, etl_batch_date, etl_batch_date))

        # Commit the transaction
        connection.commit()
        print("Data successfully updated and inserted into devdw.DPS.")

    except Exception as e:
        print(f"Error during Redshift operations: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Fetch the ETL batch date before calling the load function
etl_batch_no,etl_batch_date = get_etl_batch_info()
if etl_batch_date:
    # Call the function to load data into the Redshift table
    load_data_to_redshift('devdw', etl_batch_date)
else:
    print("ETL batch date not found. Exiting.")
