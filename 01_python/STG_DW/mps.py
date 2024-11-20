import os
import sys
import boto3
import psycopg2 
from psycopg2 import sql
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
    """Load data from devstage to devdw and update timestamps in the monthly_product_summary table."""
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

        # Update DW_UPDATE_TIMESTAMP for all records in the monthly_product_summary table
        update_query = """
        UPDATE devdw.monthly_product_summary m
            SET
                CUSTOMER_APD = d.CUSTOMER_APD,
                CUSTOMER_APM = CASE WHEN d.CUSTOMER_APD != 0 THEN 1 ELSE 0 END,
                PRODUCT_COST_AMOUNT = d.PRODUCT_COST_AMOUNT,
                PRODUCT_MRP_AMOUNT = d.PRODUCT_MRP_AMOUNT,
                CANCELLED_PRODUCT_QTY = d.CANCELLED_PRODUCT_QTY,
                CANCELLED_COST_AMOUNT = d.CANCELLED_COST_AMOUNT,
                CANCELLED_MRP_AMOUNT = d.CANCELLED_MRP_AMOUNT,
                CANCELLED_ORDER_APD = d.CANCELLED_ORDER_APD,
                CANCELLED_ORDER_APM = CASE WHEN d.CANCELLED_ORDER_APD != 0 THEN 1 ELSE 0 END,
                ETL_BATCH_NO = %s,
                ETL_BATCH_DATE = %s
            FROM (
                SELECT 
                    DATE_TRUNC('month', SUMMARY_DATE) AS START_OF_THE_MONTH_DATE,
                    DW_PRODUCT_ID,
                    SUM(CUSTOMER_APD) AS CUSTOMER_APD,
                    SUM(PRODUCT_COST_AMOUNT) AS PRODUCT_COST_AMOUNT,
                    SUM(PRODUCT_MRP_AMOUNT) AS PRODUCT_MRP_AMOUNT,
                    SUM(CANCELLED_PRODUCT_QTY) AS CANCELLED_PRODUCT_QTY,
                    SUM(CANCELLED_COST_AMOUNT) AS CANCELLED_COST_AMOUNT,
                    SUM(CANCELLED_MRP_AMOUNT) AS CANCELLED_MRP_AMOUNT,
                    SUM(CANCELLED_ORDER_APD) AS CANCELLED_ORDER_APD
                FROM devdw.daily_product_summary
                GROUP BY 
                    DATE_TRUNC('month', SUMMARY_DATE),
                    DW_PRODUCT_ID
            ) d
            WHERE 
                m.START_OF_THE_MONTH_DATE = d.START_OF_THE_MONTH_DATE
                AND m.DW_PRODUCT_ID = d.DW_PRODUCT_ID;

         """
        cursor.execute(update_query,(etl_batch_no, etl_batch_date))
        
        # Insert new records into the monthly_product_summary table from the devstage schema
        insert_query = """
        INSERT INTO devdw.monthly_product_summary (
        START_OF_THE_MONTH_DATE,
        DW_PRODUCT_ID,
        CUSTOMER_APD,
        CUSTOMER_APM, 
        PRODUCT_COST_AMOUNT,
        PRODUCT_MRP_AMOUNT,
        CANCELLED_PRODUCT_QTY,
        CANCELLED_COST_AMOUNT,
        CANCELLED_MRP_AMOUNT,
        CANCELLED_ORDER_APD,
        CANCELLED_ORDER_APM,
        ETL_BATCH_NO,
        ETL_BATCH_DATE,
        DW_CREATE_TIMESTAMP,
        DW_UPDATE_TIMESTAMP
        )
        SELECT
        DATE_TRUNC('month', DPS.SUMMARY_DATE) AS START_OF_THE_MONTH_DATE,
        DPS.DW_PRODUCT_ID, 
        SUM(DPS.CUSTOMER_APD),
        CASE WHEN MAX(DPS.CUSTOMER_APD) > 0 THEN 1 ELSE 0 END AS CUSTOMER_APM,
        SUM(DPS.PRODUCT_COST_AMOUNT),
        SUM(DPS.PRODUCT_MRP_AMOUNT),
        SUM(DPS.CANCELLED_PRODUCT_QTY),
        SUM(DPS.CANCELLED_COST_AMOUNT),
        SUM(DPS.CANCELLED_MRP_AMOUNT),
        SUM(DPS.CANCELLED_ORDER_APD),
        CASE WHEN MAX(DPS.CANCELLED_ORDER_APD) > 0 THEN 1 ELSE 0 END AS CANCELLED_ORDER_APM,
        %s,
        %s,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
        FROM devdw.daily_product_summary DPS
        LEFT JOIN devdw.monthly_product_summary MPS
        ON DATE_TRUNC('month', DPS.SUMMARY_DATE) = MPS.START_OF_THE_MONTH_DATE
        AND DPS.DW_PRODUCT_ID = MPS.DW_PRODUCT_ID
        WHERE MPS.DW_PRODUCT_ID IS NULL
        GROUP BY DATE_TRUNC('month', DPS.SUMMARY_DATE), DPS.DW_PRODUCT_ID;
            
        """
        cursor.execute(insert_query,(etl_batch_no, etl_batch_date))

        # Commit the transaction
        connection.commit()
        print("Data successfully updated and inserted into devdw.monthly_product_summary.")

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
