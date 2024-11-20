# import os
# import sys
# import boto3
# import psycopg2
# sys.path.append(os.path.abspath(".."))
# from config import redshift_username, redshift_password, redshift_dsn, bucket, tables
# from dotenv import load_dotenv

# # Load environment variables
# load_dotenv()

# # AWS S3 and Redshift Setup
# client = boto3.client('s3')

# def get_etl_batch_info():
#     """Fetch the ETL batch date and batch number from Redshift."""
#     connection = None
#     cursor = None
#     try:
#         connection = psycopg2.connect(
#             dbname=redshift_dsn['dbname'],
#             user=redshift_username,
#             password=redshift_password,
#             host=redshift_dsn['host'],
#             port=redshift_dsn['port']
#         )
        
#         cursor = connection.cursor()
#         etl_batch_info_query = "SELECT etl_batch_no, etl_batch_date FROM metadata.batch_control"
#         cursor.execute(etl_batch_info_query)
#         result = cursor.fetchone()
        
#         if result is None:
#             print("No ETL batch information found in the batch_control table.")
#             return None, None
        
#         # Return both the batch number and date
#         return result[0], result[1]  # etl_batch_no, etl_batch_date
#     except Exception as e:
#         print(f"Error fetching batch information from Redshift: {e}")
#         return None, None
#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()

# def load_data_to_redshift(schema_name, etl_batch_date):
#     """Load data from devstage to devdw and update timestamps in the orderdetails table."""
#     connection = None
#     cursor = None
#     try:
#         connection = psycopg2.connect(
#             dbname=redshift_dsn['dbname'],
#             user=redshift_username,
#             password=redshift_password,
#             host=redshift_dsn['host'],
#             port=redshift_dsn['port']
#         )
#         cursor = connection.cursor()

#         # Update DW_UPDATE_TIMESTAMP for all records in the orderdetails table
#         update_query = """
#         UPDATE devdw.ORDERDETAILS  B
#         SET 
#             QUANTITYORDERED = A.QUANTITYORDERED,
#             DW_ORDER_ID= O.DW_ORDER_ID,
#             DW_PRODUCT_ID=P.DW_PRODUCT_ID,
#             PRICEEACH = A.PRICEEACH,
#             ORDERLINENUMBER = A.ORDERLINENUMBER,
#             SRC_UPDATE_TIMESTAMP = A.UPDATE_TIMESTAMP,
#             DW_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP,
#             ETL_BATCH_NO=%s,
#             ETL_BATCH_DATE=%s
#             FROM devstage.ORDERDETAILS AS A 
#             JOIN devdw.PRODUCTS AS P ON  A.PRODUCTCODE = P.SRC_PRODUCTCODE
#             JOIN devdw.ORDERS AS O ON  A.ORDERNUMBER = O.SRC_ORDERNUMBER 
#             WHERE A.ORDERNUMBER = B.SRC_ORDERNUMBER AND A.PRODUCTCODE=B.SRC_PRODUCTCODE ;
#         """
#         cursor.execute(update_query,(etl_batch_no, etl_batch_date))
        
#         # Insert new records into the orderdetails table from the devstage schema
#         insert_query = """
        
#         INSERT INTO devdw.ORDERDETAILS (
#             SRC_ORDERNUMBER, 
#             DW_ORDER_ID,
#             DW_PRODUCT_ID,
#             SRC_PRODUCTCODE, 
#             QUANTITYORDERED, 
#             PRICEEACH, 
#             ORDERLINENUMBER, 
#             SRC_CREATE_TIMESTAMP, 
#             SRC_UPDATE_TIMESTAMP,
#             ETL_BATCH_NO,
#             ETL_BATCH_DATE

#         )
#         SELECT 
#             A.ORDERNUMBER,
#             O.DW_ORDER_ID,
#             P.DW_PRODUCT_ID,
#             A.PRODUCTCODE,
#             A.QUANTITYORDERED,
#             A.PRICEEACH,
#             A.ORDERLINENUMBER,
#             A.CREATE_TIMESTAMP AS SRC_CREATE_TIMESTAMP,
#             A.UPDATE_TIMESTAMP AS SRC_UPDATE_TIMESTAMP,
#             %s,
#             %s
#         FROM 
#             devstage.ORDERDETAILS A
#         JOIN 
#             devdw.ORDERS O ON O.SRC_ORDERNUMBER = A.ORDERNUMBER
#         JOIN 
#             devdw.PRODUCTS P ON  A.PRODUCTCODE = P.SRC_PRODUCTCODE
#         LEFT JOIN 
#             devdw.ORDERDETAILS B ON B.SRC_ORDERNUMBER =A.ORDERNUMBER  AND B.SRC_PRODUCTCODE=A.PRODUCTCODE
#         WHERE 
#         B.SRC_ORDERNUMBER IS NULL AND B.SRC_PRODUCTCODE IS NULL;
#         """
#         cursor.execute(insert_query,(etl_batch_no, etl_batch_date))

#         # Commit the transaction
#         connection.commit()
#         print("Data successfully updated and inserted into devdw.orderdetails.")

#     except Exception as e:
#         print(f"Error during Redshift operations: {e}")
#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()

# # Fetch the ETL batch date before calling the load function
# etl_batch_no,etl_batch_date = get_etl_batch_info()
# if etl_batch_date:
#     # Call the function to load data into the Redshift table
#     load_data_to_redshift('devdw', etl_batch_date)
# else:
#     print("ETL batch date not found. Exiting.")

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
    """Load data from devstage to devdw and update timestamps in the orderdetails table."""
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

        # Update DW_UPDATE_TIMESTAMP for all records in the orderdetails table
        update_query = """
        UPDATE devdw.orderdetails d
SET
    quantityordered = s.quantityordered,
    priceeach = s.priceeach,
    orderlinenumber = s.orderlinenumber,
    src_update_timestamp = s.update_timestamp,
    dw_update_timestamp = CURRENT_TIMESTAMP, -- Replace with the current timestamp
    dw_order_id = o.dw_order_id,
    etl_batch_no = %s, -- Placeholder for ETL batch number
    etl_batch_date = %s, -- Placeholder for ETL batch date
    dw_product_id = p.dw_product_id
FROM devstage.orderdetails s
JOIN devdw.orders o ON s.ordernumber = o.src_ordernumber
JOIN devdw.products p ON s.productcode = p.src_productcode
WHERE d.src_ordernumber = s.ordernumber AND d.src_productcode = s.productcode;

        """
        cursor.execute(update_query,(etl_batch_no, etl_batch_date))
        
        # Insert new records into the orderdetails table from the devstage schema
        insert_query = """
        INSERT INTO devdw.orderdetails (
    src_ordernumber, src_productcode, quantityordered, priceeach,
    orderlinenumber, src_create_timestamp, src_update_timestamp,
    dw_order_id, dw_product_id, etl_batch_no, etl_batch_date
)
SELECT
    s.ordernumber, s.productcode, s.quantityordered, s.priceeach,
    s.orderlinenumber, s.create_timestamp, s.update_timestamp,
    o.dw_order_id, p.dw_product_id, %s, %s
FROM devstage.orderdetails s
JOIN devdw.orders o ON s.ordernumber = o.src_ordernumber
JOIN devdw.products p ON s.productcode = p.src_productcode
LEFT JOIN devdw.orderdetails d 
    ON s.ordernumber = d.src_ordernumber AND s.productcode = d.src_productcode
WHERE d.src_ordernumber IS NULL AND d.src_productcode IS NULL;
       
        """
        cursor.execute(insert_query,(etl_batch_no, etl_batch_date))

        # Commit the transaction
        connection.commit()
        print("Data successfully updated and inserted into devdw.orderdetails.")

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
