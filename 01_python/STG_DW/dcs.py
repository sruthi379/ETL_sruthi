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
    """Load data from devstage to devdw and update timestamps in the DCS table."""
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

        # Insert new records into the DCS table from the devstage schema

        insert_query = """
        INSERT INTO devdw.daily_customer_summary (
        SUMMARY_DATE,
        DW_CUSTOMER_ID,
        ORDER_COUNT,
        ORDER_APD,
        ORDER_COST_AMOUNT,
        CANCELLED_ORDER_COUNT,
        CANCELLED_ORDER_AMOUNT,
        CANCELLED_ORDER_APD,
        SHIPPED_ORDER_COUNT,
        SHIPPED_ORDER_AMOUNT,
        SHIPPED_ORDER_APD,
        PAYMENT_APD,
        PAYMENT_AMOUNT,
        PRODUCTS_ORDERED_QTY,
        PRODUCTS_ITEMS_QTY,
        ORDER_MRP_AMOUNT,
        NEW_CUSTOMER_APD,
        NEW_CUSTOMER_PAID_APD,
        DW_CREATE_TIMESTAMP,
        DW_UPDATE_TIMESTAMP,
        ETL_BATCH_NO,
        ETL_BATCH_DATE
    )
    SELECT SUMMARY_DATE,
        DW_CUSTOMER_ID,
        MAX(ORDER_COUNT) AS ORDER_COUNT,
        MAX(ORDER_APD) AS ORDER_APD,
        MAX(ORDER_COST_AMOUNT) AS ORDER_COST_AMOUNT,
        MAX(CANCELLED_ORDER_COUNT) AS CANCELLED_ORDER_COUNT,
        MAX(CANCELLED_ORDER_AMOUNT) AS CANCELLED_ORDER_AMOUNT,
        MAX(CANCELLED_ORDER_APD) AS CANCELLED_ORDER_APD,
        MAX(SHIPPED_ORDER_COUNT) AS SHIPPED_ORDER_COUNT,
        MAX(SHIPPED_ORDER_AMOUNT) AS SHIPPED_ORDER_AMOUNT,
        MAX(SHIPPED_ORDER_APD) AS SHIPPED_ORDER_APD,
        MAX(PAYMENT_APD) AS PAYMENT_APD,
        MAX(PAYMENT_AMOUNT) AS PAYMENT_AMOUNT,
        MAX(PRODUCTS_ORDERED_QTY) AS PRODUCTS_ORDERED_QTY,
        MAX(PRODUCTS_ITEMS_QTY) AS PRODUCTS_ITEMS_QTY,
        MAX(ORDER_MRP_AMOUNT) AS ORDER_MRP_AMOUNT,
        MAX(NEW_CUSTOMER_APD) AS NEW_CUSTOMER_APD,
        MAX(NEW_CUSTOMER_PAID_APD) AS NEW_CUSTOMER_PAID_APD,
        CURRENT_TIMESTAMP AS DW_CREATE_TIMESTAMP,
        CURRENT_TIMESTAMP AS DW_UPDATE_TIMESTAMP,
        %s AS ETL_BATCH_NO,
        %s AS ETL_BATCH_DATE
    FROM (
        SELECT CAST(C.SRC_CREATE_TIMESTAMP AS DATE) AS SUMMARY_DATE,
            C.DW_CUSTOMER_ID,
            0 AS ORDER_COUNT,
            0 AS ORDER_APD,
            0 AS ORDER_COST_AMOUNT,
            0 AS CANCELLED_ORDER_COUNT,
            0 AS CANCELLED_ORDER_AMOUNT,
            0 AS CANCELLED_ORDER_APD,
            0 AS SHIPPED_ORDER_COUNT,
            0 AS SHIPPED_ORDER_AMOUNT,
            0 AS SHIPPED_ORDER_APD,
            0 AS PAYMENT_APD,
            0 AS PAYMENT_AMOUNT,
            0 AS PRODUCTS_ORDERED_QTY,
            0 AS PRODUCTS_ITEMS_QTY,
            0 AS ORDER_MRP_AMOUNT,
            1 AS NEW_CUSTOMER_APD,
            0 AS NEW_CUSTOMER_PAID_APD
        FROM devdw.CUSTOMERS C
        WHERE CAST(C.SRC_CREATE_TIMESTAMP AS DATE) >= %s

        UNION ALL

        SELECT CAST(O.CANCELLEDDATE AS DATE) AS SUMMARY_DATE,
            O.DW_CUSTOMER_ID,
            0 AS ORDER_COUNT,
            0 AS ORDER_APD,
            0 AS ORDER_COST_AMOUNT,
            COUNT(O.DW_ORDER_ID) AS CANCELLED_ORDER_COUNT,
            SUM(OD.PRICEEACH * OD.QUANTITYORDERED) AS CANCELLED_ORDER_AMOUNT,
            1 AS CANCELLED_ORDER_APD,
            0 AS SHIPPED_ORDER_COUNT,
            0 AS SHIPPED_ORDER_AMOUNT,
            0 AS SHIPPED_ORDER_APD,
            0 AS PAYMENT_APD,
            0 AS PAYMENT_AMOUNT,
            0 AS PRODUCTS_ORDERED_QTY,
            0 AS PRODUCTS_ITEMS_QTY,
            0 AS ORDER_MRP_AMOUNT,
            0 AS NEW_CUSTOMER_APD,
            0 AS NEW_CUSTOMER_PAID_APD
        FROM devdw.ORDERS O
        JOIN devdw.ORDERDETAILS OD ON O.DW_ORDER_ID = OD.DW_ORDER_ID
        WHERE CAST(O.CANCELLEDDATE AS DATE) >= %s
        AND O.STATUS = 'Cancelled'
        GROUP BY O.DW_CUSTOMER_ID, CAST(O.CANCELLEDDATE AS DATE)

        UNION ALL

        SELECT CAST(P.PAYMENTDATE AS DATE) AS SUMMARY_DATE,
            P.DW_CUSTOMER_ID,
            0 AS ORDER_COUNT,
            0 AS ORDER_APD,
            0 AS ORDER_COST_AMOUNT,
            0 AS CANCELLED_ORDER_COUNT,
            0 AS CANCELLED_ORDER_AMOUNT,
            0 AS CANCELLED_ORDER_APD,
            0 AS SHIPPED_ORDER_COUNT,
            0 AS SHIPPED_ORDER_AMOUNT,
            0 AS SHIPPED_ORDER_APD,
            1 AS PAYMENT_APD,
            SUM(P.AMOUNT) AS PAYMENT_AMOUNT,
            0 AS PRODUCTS_ORDERED_QTY,
            0 AS PRODUCTS_ITEMS_QTY,
            0 AS ORDER_MRP_AMOUNT,
            0 AS NEW_CUSTOMER_APD,
            0 AS NEW_CUSTOMER_PAID_APD
        FROM devdw.PAYMENTS P
        WHERE CAST(P.PAYMENTDATE AS DATE) >= %s
        GROUP BY P.DW_CUSTOMER_ID, CAST(P.PAYMENTDATE AS DATE)

        UNION ALL

        SELECT CAST(O.SHIPPEDDATE AS DATE) AS SUMMARY_DATE,
            O.DW_CUSTOMER_ID,
            0 AS ORDER_COUNT,
            0 AS ORDER_APD,
            0 AS ORDER_COST_AMOUNT,
            0 AS CANCELLED_ORDER_COUNT,
            0 AS CANCELLED_ORDER_AMOUNT,
            0 AS CANCELLED_ORDER_APD,
            COUNT(DISTINCT O.DW_ORDER_ID) AS SHIPPED_ORDER_COUNT,
            SUM(OD.PRICEEACH * OD.QUANTITYORDERED) AS SHIPPED_ORDER_AMOUNT,
            1 AS SHIPPED_ORDER_APD,
            0 AS PAYMENT_APD,
            0 AS PAYMENT_AMOUNT,
            0 AS PRODUCTS_ORDERED_QTY,
            0 AS PRODUCTS_ITEMS_QTY,
            0 AS ORDER_MRP_AMOUNT,
            0 AS NEW_CUSTOMER_APD,
            0 AS NEW_CUSTOMER_PAID_APD
        FROM devdw.ORDERS O
        JOIN devdw.ORDERDETAILS OD ON O.DW_ORDER_ID = OD.DW_ORDER_ID
        WHERE CAST(O.SHIPPEDDATE AS DATE) >= %s
        AND O.STATUS = 'Shipped'
        GROUP BY CAST(O.SHIPPEDDATE AS DATE), O.DW_CUSTOMER_ID

        UNION ALL

        SELECT CAST(O.ORDERDATE AS DATE) AS SUMMARY_DATE,
            O.DW_CUSTOMER_ID,
            COUNT(DISTINCT O.DW_ORDER_ID) AS ORDER_COUNT,
            1 AS ORDER_APD,
            SUM(OD.PRICEEACH * OD.QUANTITYORDERED) AS ORDER_COST_AMOUNT,
            0 AS CANCELLED_ORDER_COUNT,
            0 AS CANCELLED_ORDER_AMOUNT,
            0 AS CANCELLED_ORDER_APD,
            0 AS SHIPPED_ORDER_COUNT,
            0 AS SHIPPED_ORDER_AMOUNT,
            0 AS SHIPPED_ORDER_APD,
            0 AS PAYMENT_APD,
            0 AS PAYMENT_AMOUNT,
            COUNT(OD.SRC_PRODUCTCODE) AS PRODUCTS_ORDERED_QTY,
            COUNT(DISTINCT P.PRODUCTLINE) AS PRODUCTS_ITEMS_QTY,
            SUM(OD.QUANTITYORDERED * P.MSRP) AS ORDER_MRP_AMOUNT,
            0 AS NEW_CUSTOMER_APD,
            0 AS NEW_CUSTOMER_PAID_APD
        FROM devdw.ORDERS O
        JOIN devdw.ORDERDETAILS OD ON O.DW_ORDER_ID = OD.DW_ORDER_ID
        JOIN devdw.PRODUCTS P ON OD.SRC_PRODUCTCODE = P.SRC_PRODUCTCODE
        WHERE CAST(O.ORDERDATE AS DATE) >= %s
        GROUP BY CAST(O.ORDERDATE AS DATE), O.DW_CUSTOMER_ID
    ) x
    GROUP BY SUMMARY_DATE, DW_CUSTOMER_ID;

        """

        cursor.execute(insert_query, (etl_batch_no, etl_batch_date, etl_batch_date, etl_batch_date, etl_batch_date, etl_batch_date,etl_batch_date))

        # Commit the transaction
        connection.commit()
        print("Data successfully updated and inserted into devdw.DCS.")

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
