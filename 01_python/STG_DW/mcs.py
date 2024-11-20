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
    """Load data from devstage to devdw and update timestamps in the mcs table."""
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

        # Update DW_UPDATE_TIMESTAMP for all records in the mcs table
        update_query = """
        UPDATE devdw.monthly_customer_summary m
        SET
            ORDER_COUNT = d.ORDER_COUNT,
            ORDER_APD = d.ORDER_APD,
            ORDER_APM = CASE WHEN d.ORDER_APD != 0 THEN 1 ELSE 0 END,
            ORDER_COST_AMOUNT = d.ORDER_COST_AMOUNT,
            CANCELLED_ORDER_COUNT = d.CANCELLED_ORDER_COUNT,
            CANCELLED_ORDER_AMOUNT = d.CANCELLED_ORDER_AMOUNT,
            CANCELLED_ORDER_APD = d.CANCELLED_ORDER_APD,
            CANCELLED_ORDER_APM = CASE WHEN d.CANCELLED_ORDER_APD != 0 THEN 1 ELSE 0 END,
            SHIPPED_ORDER_COUNT = d.SHIPPED_ORDER_COUNT,
            SHIPPED_ORDER_AMOUNT = d.SHIPPED_ORDER_AMOUNT,
            SHIPPED_ORDER_APD = d.SHIPPED_ORDER_APD,
            SHIPPED_ORDER_APM = CASE WHEN d.SHIPPED_ORDER_APD != 0 THEN 1 ELSE 0 END,
            PAYMENT_APD = d.PAYMENT_APD,
            PAYMENT_APM = CASE WHEN d.PAYMENT_APD != 0 THEN 1 ELSE 0 END,
            PAYMENT_AMOUNT = d.PAYMENT_AMOUNT,
            PRODUCTS_ORDERED_QTY = d.PRODUCTS_ORDERED_QTY,
            PRODUCTS_ITEMS_QTY = d.PRODUCTS_ITEMS_QTY,
            ORDER_MRP_AMOUNT = d.ORDER_MRP_AMOUNT,
            NEW_CUSTOMER_APD = d.NEW_CUSTOMER_APD,
            NEW_CUSTOMER_APM = CASE WHEN d.NEW_CUSTOMER_APD != 0 THEN 1 ELSE 0 END,
            NEW_CUSTOMER_PAID_APD = d.NEW_CUSTOMER_PAID_APD,
            NEW_CUSTOMER_PAID_APM = CASE WHEN d.NEW_CUSTOMER_PAID_APD != 0 THEN 1 ELSE 0 END,
            DW_UPDATE_TIMESTAMP = d.DW_UPDATE_TIMESTAMP,
            ETL_BATCH_NO = d.ETL_BATCH_NO,
            ETL_BATCH_DATE = d.ETL_BATCH_DATE
        FROM (
            SELECT 
                DATE_TRUNC('month', SUMMARY_DATE) AS START_OF_THE_MONTH_DATE,
                DW_CUSTOMER_ID,
                SUM(ORDER_COUNT) AS ORDER_COUNT,
                SUM(ORDER_APD) AS ORDER_APD,  
                SUM(ORDER_COST_AMOUNT) AS ORDER_COST_AMOUNT,
                SUM(CANCELLED_ORDER_COUNT) AS CANCELLED_ORDER_COUNT,
                SUM(CANCELLED_ORDER_AMOUNT) AS CANCELLED_ORDER_AMOUNT,
                SUM(CANCELLED_ORDER_APD) AS CANCELLED_ORDER_APD,  
                SUM(SHIPPED_ORDER_COUNT) AS SHIPPED_ORDER_COUNT,
                SUM(SHIPPED_ORDER_AMOUNT) AS SHIPPED_ORDER_AMOUNT,
                SUM(SHIPPED_ORDER_APD) AS SHIPPED_ORDER_APD,  
                SUM(PAYMENT_APD) AS PAYMENT_APD,  
                SUM(PAYMENT_AMOUNT) AS PAYMENT_AMOUNT,
                SUM(PRODUCTS_ORDERED_QTY) AS PRODUCTS_ORDERED_QTY,
                SUM(PRODUCTS_ITEMS_QTY) AS PRODUCTS_ITEMS_QTY,
                SUM(ORDER_MRP_AMOUNT) AS ORDER_MRP_AMOUNT,
                SUM(NEW_CUSTOMER_APD) AS NEW_CUSTOMER_APD,
                SUM(NEW_CUSTOMER_PAID_APD) AS NEW_CUSTOMER_PAID_APD,  
                CURRENT_TIMESTAMP AS DW_UPDATE_TIMESTAMP,
                %s AS ETL_BATCH_NO,
                %s AS ETL_BATCH_DATE
            FROM devdw.daily_customer_summary
            GROUP BY 
                DATE_TRUNC('month', SUMMARY_DATE),
                DW_CUSTOMER_ID
        ) d
        WHERE 
            m.START_OF_THE_MONTH_DATE = d.START_OF_THE_MONTH_DATE
            AND m.DW_CUSTOMER_ID = d.DW_CUSTOMER_ID;

        """
        cursor.execute(update_query,(etl_batch_no, etl_batch_date))
        
        # Insert new records into the mcs table from the devstage schema
        insert_query = """
        
        INSERT INTO devdw.monthly_customer_summary
        (
            START_OF_THE_MONTH_DATE,
            DW_CUSTOMER_ID,
            ORDER_COUNT,
            ORDER_APD,
            ORDER_APM,
            ORDER_COST_AMOUNT,
            CANCELLED_ORDER_COUNT,
            CANCELLED_ORDER_AMOUNT,
            CANCELLED_ORDER_APD,
            CANCELLED_ORDER_APM,
            SHIPPED_ORDER_COUNT,
            SHIPPED_ORDER_AMOUNT,
            SHIPPED_ORDER_APD,
            SHIPPED_ORDER_APM,
            PAYMENT_APD,
            PAYMENT_APM,
            PAYMENT_AMOUNT,
            PRODUCTS_ORDERED_QTY,
            PRODUCTS_ITEMS_QTY,
            ORDER_MRP_AMOUNT,
            NEW_CUSTOMER_APD,
            NEW_CUSTOMER_APM,
            NEW_CUSTOMER_PAID_APD,
            NEW_CUSTOMER_PAID_APM,
            DW_CREATE_TIMESTAMP,
            DW_UPDATE_TIMESTAMP,
            ETL_BATCH_NO,
            ETL_BATCH_DATE
        )
        SELECT 
            DATE_TRUNC('month', SUMMARY_DATE) AS START_OF_THE_MONTH_DATE,
            d.DW_CUSTOMER_ID,
            SUM(d.ORDER_COUNT) AS ORDER_COUNT,
            MAX(d.ORDER_APD) AS ORDER_APD,  
            MAX(d.ORDER_APD) AS ORDER_APM, 
            SUM(d.ORDER_COST_AMOUNT) AS ORDER_COST_AMOUNT,
            SUM(d.CANCELLED_ORDER_COUNT) AS CANCELLED_ORDER_COUNT,
            SUM(d.CANCELLED_ORDER_AMOUNT) AS CANCELLED_ORDER_AMOUNT,
            SUM(d.CANCELLED_ORDER_APD) AS CANCELLED_ORDER_APD,  
            MAX(CASE WHEN d.CANCELLED_ORDER_APD != 0 THEN 1 ELSE 0 END) AS CANCELLED_ORDER_APM,  
            SUM(d.SHIPPED_ORDER_COUNT) AS SHIPPED_ORDER_COUNT,
            SUM(d.SHIPPED_ORDER_AMOUNT) AS SHIPPED_ORDER_AMOUNT,
            SUM(d.SHIPPED_ORDER_APD) AS SHIPPED_ORDER_APD,  
            MAX(CASE WHEN d.SHIPPED_ORDER_APD != 0 THEN 1 ELSE 0 END) AS SHIPPED_ORDER_APM, 
            SUM(d.PAYMENT_APD) AS PAYMENT_APD,  
            MAX(CASE WHEN d.PAYMENT_APD != 0 THEN 1 ELSE 0 END) AS PAYMENT_APM, 
            SUM(d.PAYMENT_AMOUNT) AS PAYMENT_AMOUNT,
            SUM(d.PRODUCTS_ORDERED_QTY) AS PRODUCTS_ORDERED_QTY,
            SUM(d.PRODUCTS_ITEMS_QTY) AS PRODUCTS_ITEMS_QTY,
            SUM(d.ORDER_MRP_AMOUNT) AS ORDER_MRP_AMOUNT,
            SUM(d.NEW_CUSTOMER_APD) AS NEW_CUSTOMER_APD,
            MAX(CASE WHEN d.NEW_CUSTOMER_APD != 0 THEN 1 ELSE 0 END) AS NEW_CUSTOMER_APM,  
            MAX(d.NEW_CUSTOMER_PAID_APD) AS NEW_CUSTOMER_PAID_APD,  
            MAX(CASE WHEN d.NEW_CUSTOMER_PAID_APD != 0 THEN 1 ELSE 0 END) AS NEW_CUSTOMER_PAID_APM,
            CURRENT_TIMESTAMP AS DW_CREATE_TIMESTAMP,
            CURRENT_TIMESTAMP AS DW_UPDATE_TIMESTAMP,
            %s AS ETL_BATCH_NO,
            %s AS ETL_BATCH_DATE
        FROM devdw.daily_customer_summary d
        LEFT JOIN devdw.monthly_customer_summary m
            ON m.START_OF_THE_MONTH_DATE = DATE_TRUNC('month', SUMMARY_DATE)
            AND m.DW_CUSTOMER_ID = d.DW_CUSTOMER_ID
        WHERE m.START_OF_THE_MONTH_DATE IS NULL
        GROUP BY 
            DATE_TRUNC('month', SUMMARY_DATE),
            d.DW_CUSTOMER_ID;

        """
        cursor.execute(insert_query,(etl_batch_no, etl_batch_date))

        # Commit the transaction
        connection.commit()
        print("Data successfully updated and inserted into devdw.MCS.")

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
