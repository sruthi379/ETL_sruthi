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
#     """Load data from devstage to devdw and update timestamps in the orders table."""
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

#         # Update DW_UPDATE_TIMESTAMP for all records in the orders table
#         update_query = """
        
#         UPDATE devdw.ORDERS B
#         SET 
#             ORDERDATE=A.ORDERDATE,
#             SRC_ORDERNUMBER=A.ORDERNUMBER,
#             REQUIREDDATE = A.REQUIREDDATE,
#             SHIPPEDDATE = A.SHIPPEDDATE,
#             STATUS = A.STATUS,
#             COMMENTS = A.COMMENTS,
#             SRC_CUSTOMERNUMBER = A.CUSTOMERNUMBER,
#             SRC_UPDATE_TIMESTAMP = A.UPDATE_TIMESTAMP,
#             DW_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP,
#             CANCELLEDDATE=A.CANCELLEDDATE,
#             ETL_BATCH_NO=%s,
#             ETL_BATCH_DATE=%s
#         FROM devstage.ORDERS A 
#         WHERE A.ORDERNUMBER = B.SRC_ORDERNUMBER;
#     """
#         cursor.execute(update_query,(etl_batch_no, etl_batch_date))
        
#         # Insert new records into the orders table from the devstage schema
#         insert_query = """
        
#         INSERT INTO devdw.ORDERS (
#             SRC_ORDERNUMBER, 
#             DW_CUSTOMER_ID,
#             ORDERDATE, 
#             REQUIREDDATE, 
#             SHIPPEDDATE, 
#             STATUS, 
#             COMMENTS, 
#             SRC_CUSTOMERNUMBER, 
#             SRC_CREATE_TIMESTAMP, 
#             SRC_UPDATE_TIMESTAMP,
#             DW_CREATE_TIMESTAMP,
#             DW_UPDATE_TIMESTAMP,
#             CANCELLEDDATE,
#             ETL_BATCH_NO,
#             ETL_BATCH_DATE

#         )
#         SELECT 
#             A.ORDERNUMBER,
#             C.DW_CUSTOMER_ID AS DW_CUSTOMER_ID ,
#             A.ORDERDATE,
#             A.REQUIREDDATE,
#             A.SHIPPEDDATE,
#             A.STATUS,
#             A.COMMENTS,
#             A.CUSTOMERNUMBER AS SRC_CUSTOMERNUMBER,
#             A.CREATE_TIMESTAMP AS SRC_CREATE_TIMESTAMP,
#             A.UPDATE_TIMESTAMP AS SRC_UPDATE_TIMESTAMP,
#             CURRENT_TIMESTAMP AS DW_CREATE_TIMESTAMP,
#             CURRENT_TIMESTAMP AS DW_UPDATE_TIMESTAMP,
#             A.CANCELLEDDATE,
#             %s,
#             %s
#         FROM 
#             devstage.ORDERS A
#         LEFT JOIN 
#             devdw.ORDERS B ON A.ORDERNUMBER = B.SRC_ORDERNUMBER
#         JOIN   devdw.CUSTOMERS C ON C.SRC_CUSTOMERNUMBER=A.CUSTOMERNUMBER
#         WHERE 
#         B.SRC_CUSTOMERNUMBER IS NULL;

#         """
#         cursor.execute(insert_query,(etl_batch_no, etl_batch_date))

#         # Commit the transaction
#         connection.commit()
#         print("Data successfully updated and inserted into devdw.orders.")

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
    """Load data from devstage to devdw and update timestamps in the orders table."""
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

        # Update DW_UPDATE_TIMESTAMP for all records in the orders table
        update_query = """
        UPDATE devdw.orders d
SET
    orderdate = s.orderdate,
    requireddate = s.requireddate,
    shippeddate = s.shippeddate,
    status = s.status,
    cancelleddate = s.cancelleddate,
    src_customernumber = s.customernumber,
    dw_customer_id = c.dw_customer_id,
    src_update_timestamp = s.update_timestamp,
    etl_batch_no = %s, -- Placeholder for ETL batch number
    etl_batch_date = %s, -- Placeholder for ETL batch date
    dw_update_timestamp = CURRENT_TIMESTAMP
FROM devstage.orders s
JOIN devdw.customers c ON s.customernumber = c.src_customernumber
WHERE d.src_ordernumber = s.ordernumber;
       
    """
        cursor.execute(update_query,(etl_batch_no, etl_batch_date))
        
        # Insert new records into the orders table from the devstage schema
        insert_query = """
        
        INSERT INTO devdw.orders (
    src_ordernumber, orderdate, requireddate, shippeddate, status,
    src_customernumber, src_create_timestamp, src_update_timestamp,
    cancelleddate, dw_customer_id, etl_batch_no, etl_batch_date
)
SELECT
    s.ordernumber, s.orderdate, s.requireddate, s.shippeddate, s.status,
    s.customernumber, s.create_timestamp, s.update_timestamp,
    s.cancelleddate, c.dw_customer_id, %s, %s
FROM devstage.orders s
JOIN devdw.customers c ON s.customernumber = c.src_customernumber
LEFT JOIN devdw.orders d ON s.ordernumber = d.src_ordernumber
WHERE d.src_ordernumber IS NULL;
        """
        cursor.execute(insert_query,(etl_batch_no, etl_batch_date))

        # Commit the transaction
        connection.commit()
        print("Data successfully updated and inserted into devdw.orders.")

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
