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
        

#     UPDATE devdw.CUSTOMERS B
#     SET 
#         CUSTOMERNAME = A.CUSTOMERNAME,
#         CONTACTLASTNAME = A.CONTACTLASTNAME,
#         CONTACTFIRSTNAME = A.CONTACTFIRSTNAME,
#         PHONE = A.PHONE,
#         ADDRESSLINE1 = A.ADDRESSLINE1,
#         ADDRESSLINE2 = A.ADDRESSLINE2,
#         CITY = A.CITY,
#         STATE = A.STATE,
#         POSTALCODE = A.POSTALCODE,
#         COUNTRY = A.COUNTRY,
#         SALESREPEMPLOYEENUMBER = A.SALESREPEMPLOYEENUMBER,
#         CREDITLIMIT = A.CREDITLIMIT,
#         SRC_UPDATE_TIMESTAMP = A.UPDATE_TIMESTAMP,
#         DW_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP,
#         ETL_BATCH_NO = %s,
#         ETL_BATCH_DATE = %s
#         FROM devstage.CUSTOMERS A
#         WHERE A.CUSTOMERNUMBER = B.SRC_CUSTOMERNUMBER;

#         """
#         cursor.execute(update_query,(etl_batch_no, etl_batch_date))
        
#         # Insert new records into the customers table from the devstage schema
#         insert_query = """
#         INSERT INTO devdw.CUSTOMERS (
#             SRC_CUSTOMERNUMBER, 
#             CUSTOMERNAME, 
#             CONTACTLASTNAME, 
#             CONTACTFIRSTNAME, 
#             PHONE, 
#             ADDRESSLINE1, 
#             ADDRESSLINE2, 
#             CITY, 
#             STATE, 
#             POSTALCODE, 
#             COUNTRY,
#             SALESREPEMPLOYEENUMBER,
#             CREDITLIMIT,
#             SRC_CREATE_TIMESTAMP, 
#             SRC_UPDATE_TIMESTAMP,
#             DW_CREATE_TIMESTAMP,
#             DW_UPDATE_TIMESTAMP,
#             ETL_BATCH_NO,
#             ETL_BATCH_DATE
#         )
#         SELECT 
#             A.CUSTOMERNUMBER,
#             A.CUSTOMERNAME,
#             A.CONTACTLASTNAME,
#             A.CONTACTFIRSTNAME,
#             A.PHONE,
#             A.ADDRESSLINE1,
#             A.ADDRESSLINE2,
#             A.CITY,
#             A.STATE,
#             A.POSTALCODE,
#             A.COUNTRY,
#             A.SALESREPEMPLOYEENUMBER,
#             A.CREDITLIMIT,
#             A.CREATE_TIMESTAMP AS SRC_CREATE_TIMESTAMP,
#             A.UPDATE_TIMESTAMP AS SRC_UPDATE_TIMESTAMP,
#             CURRENT_TIMESTAMP AS DW_CREATE_TIMESTAMP,
#             CURRENT_TIMESTAMP AS DW_UPDATE_TIMESTAMP,
#             %s AS ETL_BATCH_NO,
#             %s AS ETL_BATCH_DATE
#         FROM devstage.CUSTOMERS A
#         LEFT JOIN devdw.CUSTOMERS B ON A.CUSTOMERNUMBER = B.SRC_CUSTOMERNUMBER
#         WHERE B.SRC_CUSTOMERNUMBER IS NULL
#         """
#         cursor.execute(insert_query,(etl_batch_no, etl_batch_date))

#         # Commit the transaction
#         connection.commit()
#         print("Data successfully updated and inserted into devdw.CUSTOMERS.")

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
        UPDATE devdw.customers d
SET
    customername = s.customername,
    contactlastname = s.contactlastname,
    contactfirstname = s.contactfirstname,
    phone = s.phone,
    addressline1 = s.addressline1,
    addressline2 = s.addressline2,
    city = s.city,
    state = s.state,
    postalcode = s.postalcode,
    country = s.country,
    salesrepemployeenumber = s.salesrepemployeenumber,
    creditlimit = s.creditlimit,
    src_update_timestamp = s.update_timestamp,
    dw_update_timestamp = CURRENT_TIMESTAMP, -- Replace with the current timestamp
    etl_batch_no = %s, -- Placeholder for ETL batch number
    etl_batch_date = %s -- Placeholder for ETL batch date
FROM devstage.customers s
WHERE d.src_customernumber = s.customernumber;



        """
        cursor.execute(update_query,(etl_batch_no, etl_batch_date))
        
        # Insert new records into the customers table from the devstage schema
        insert_query = """
        INSERT INTO devdw.customers (
    src_customernumber, customername, contactlastname, contactfirstname,
    phone, addressline1, addressline2, city, state, postalcode, country,
    salesrepemployeenumber, creditlimit, src_create_timestamp, src_update_timestamp,
    etl_batch_no, etl_batch_date
)
SELECT
    s.customernumber, s.customername, s.contactlastname, s.contactfirstname,
    s.phone, s.addressline1, s.addressline2, s.city, s.state, s.postalcode,
    s.country, s.salesrepemployeenumber, s.creditlimit, s.create_timestamp,
    s.update_timestamp, %s, %s
FROM devstage.customers s
LEFT JOIN devdw.customers d ON s.customernumber = d.src_customernumber
WHERE d.src_customernumber IS NULL;
        """
        cursor.execute(insert_query,(etl_batch_no, etl_batch_date))

        # Commit the transaction
        connection.commit()
        print("Data successfully updated and inserted into devdw.CUSTOMERS.")

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
