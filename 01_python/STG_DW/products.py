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
#     """Load data from devstage to devdw and update timestamps in the customers table."""
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

#         # Update DW_UPDATE_TIMESTAMP for all records in the customers table
#         update_query = """
        
# UPDATE devdw.PRODUCTS B
# SET 
#     PRODUCTNAME = A.PRODUCTNAME,
#     PRODUCTLINE = A.PRODUCTLINE,
#     PRODUCTSCALE = A.PRODUCTSCALE,
#     PRODUCTVENDOR = A.PRODUCTVENDOR,
#     PRODUCTDESCRIPTION = A.PRODUCTDESCRIPTION,
#     QUANTITYINSTOCK = A.QUANTITYINSTOCK,
#     BUYPRICE = A.BUYPRICE,
#     MSRP = A.MSRP,
#     SRC_UPDATE_TIMESTAMP = A.UPDATE_TIMESTAMP,
#     DW_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP,
#     ETL_BATCH_NO=%s,
#     ETL_BATCH_DATE=%s
# FROM devstage.PRODUCTS A 
# WHERE  A.PRODUCTCODE = B.SRC_PRODUCTCODE;



#         """
#         cursor.execute(update_query,(etl_batch_no, etl_batch_date))
        
#         # Insert new records into the customers table from the devstage schema
#         insert_query = """
#             INSERT INTO devdw.PRODUCTS (
#             SRC_PRODUCTCODE, 
#             PRODUCTNAME, 
#             PRODUCTLINE, 
#             PRODUCTSCALE, 
#             PRODUCTVENDOR, 
#             PRODUCTDESCRIPTION, 
#             QUANTITYINSTOCK, 
#             BUYPRICE, 
#             MSRP, 
#             SRC_CREATE_TIMESTAMP, 
#             SRC_UPDATE_TIMESTAMP,
#             DW_CREATE_TIMESTAMP,
#             DW_UPDATE_TIMESTAMP,
#             ETL_BATCH_NO,
#             ETL_BATCH_DATE

#         )
#         SELECT 
#             A.PRODUCTCODE,
#             A.PRODUCTNAME,
#             A.PRODUCTLINE,
#             A.PRODUCTSCALE,
#             A.PRODUCTVENDOR,
#             A.PRODUCTDESCRIPTION,
#             A.QUANTITYINSTOCK,
#             A.BUYPRICE,
#             A.MSRP,
#             A.CREATE_TIMESTAMP AS SRC_CREATE_TIMESTAMP,
#             A.UPDATE_TIMESTAMP AS SRC_UPDATE_TIMESTAMP,
#             CURRENT_TIMESTAMP AS DW_CREATE_TIMESTAMP,
#             CURRENT_TIMESTAMP AS DW_UPDATE_TIMESTAMP,
#             %s,
#             %s
            
#         FROM 
#             devstage.PRODUCTS A
#         LEFT JOIN 
#             devdw.PRODUCTS B ON A.PRODUCTCODE = B.SRC_PRODUCTCODE
#         WHERE 
#             B.SRC_PRODUCTCODE IS NULL;

#         """
#         cursor.execute(insert_query,(etl_batch_no, etl_batch_date))

#         # Commit the transaction
#         connection.commit()
#         print("Data successfully updated and inserted into devdw.PRODUCTS.")

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
    """Load data from devstage to devdw and update timestamps in the customers table."""
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

        # Update DW_UPDATE_TIMESTAMP for all records in the customers table
        update_query = """
        UPDATE devdw.products d
SET
    productname = s.productname,
    productline = s.productline,
    productscale = s.productscale,
    productvendor = s.productvendor,
    quantityinstock = s.quantityinstock,
    buyprice = s.buyprice,
    msrp = s.msrp,
    src_update_timestamp = s.update_timestamp,
    dw_update_timestamp = CURRENT_TIMESTAMP, -- Replace with the current timestamp
    etl_batch_no =%s, -- Placeholder for ETL batch number
    etl_batch_date = %s, -- Placeholder for ETL batch date
    dw_product_line_id = pl.dw_product_line_id
FROM devstage.products s
JOIN devdw.productlines pl ON s.productline = pl.productline
WHERE d.src_productcode = s.productcode;


        """
        cursor.execute(update_query,(etl_batch_no, etl_batch_date))
        
        # Insert new records into the customers table from the devstage schema
        insert_query = """
            

INSERT INTO devdw.products (
    src_productcode, productname, productline, productscale, productvendor,
    quantityinstock, buyprice, msrp, src_create_timestamp,
    src_update_timestamp, dw_product_line_id, etl_batch_no, etl_batch_date
)
SELECT
    s.productcode, s.productname, s.productline, s.productscale, s.productvendor,
    s.quantityinstock, s.buyprice, s.msrp, s.create_timestamp,
    s.update_timestamp, pl.dw_product_line_id, %s, %s
FROM devstage.products s
JOIN devdw.productlines pl ON s.productline = pl.productline
LEFT JOIN devdw.products d ON s.productcode = d.src_productcode
WHERE d.src_productcode IS NULL;

        """
        cursor.execute(insert_query,(etl_batch_no, etl_batch_date))

        # Commit the transaction
        connection.commit()
        print("Data successfully updated and inserted into devdw.PRODUCTS.")

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
