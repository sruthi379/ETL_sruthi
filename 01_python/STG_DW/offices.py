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
#     """Load data from devstage to devdw and update timestamps in the OFFICES table."""
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
        
#         #Update DW_UPDATE_TIMESTAMP for all records in the OFFICES table
#         update_query = """
#         UPDATE devdw.OFFICES B
#         SET 
#         CITY = A.CITY,
#         PHONE = A.PHONE,
#         ADDRESSLINE1 = A.ADDRESSLINE1,
#         ADDRESSLINE2 = A.ADDRESSLINE2,
#         STATE = A.STATE,
#         COUNTRY = A.COUNTRY,
#         POSTALCODE = A.POSTALCODE,
#         TERRITORY = A.TERRITORY,
#         SRC_UPDATE_TIMESTAMP = A.UPDATE_TIMESTAMP,
#         DW_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP,
#         ETL_BATCH_NO=%s,
#         ETL_BATCH_DATE=%s
#         FROM devstage.OFFICES A 
#         WHERE A.OFFICECODE=B.OFFICECODE
#         """
      

#         # Insert new records into the OFFICES table from the devstage schema
#         insert_query = """
#         INSERT INTO devdw.OFFICES (
#             OFFICECODE, 
#             CITY, 
#             PHONE, 
#             ADDRESSLINE1, 
#             ADDRESSLINE2, 
#             STATE, 
#             POSTALCODE, 
#             COUNTRY, 
#             TERRITORY, 
#             SRC_CREATE_TIMESTAMP, 
#             SRC_UPDATE_TIMESTAMP,
#             DW_CREATE_TIMESTAMP,
#             DW_UPDATE_TIMESTAMP,
#             ETL_BATCH_NO,
#             ETL_BATCH_DATE

#         )
#         SELECT 
#             A.OFFICECODE AS OFFICECODE,
#             A.CITY,
#             A.PHONE,
#             A.ADDRESSLINE1,
#             A.ADDRESSLINE2,
#             A.STATE,
#             A.POSTALCODE,
#             A.COUNTRY,
#             A.TERRITORY,
#             A.CREATE_TIMESTAMP AS SRC_CREATE_TIMESTAMP,
#             A.UPDATE_TIMESTAMP AS SRC_UPDATE_TIMESTAMP,
#             CURRENT_TIMESTAMP AS DW_CREATE_TIMESTAMP,
#             CURRENT_TIMESTAMP AS DW_UPDATE_TIMESTAMP,
#             %s,
#             %s
#         FROM 
#             devstage.OFFICES A
#         LEFT JOIN
#             devdw.OFFICES B ON A.OFFICECODE = B.OFFICECODE
#         WHERE 
#             B.OFFICECODE IS NULL;


#         """
        
    
#         cursor.execute(insert_query,(etl_batch_no, etl_batch_date))
#         cursor.execute(update_query,(etl_batch_no, etl_batch_date))


#         # Commit the transaction
#         connection.commit()
#         print(f"Data successfully updated and inserted into devdw.offices.")

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
    """Load data from devstage to devdw and update timestamps in the OFFICES table."""
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
        
        #Update DW_UPDATE_TIMESTAMP for all records in the OFFICES table
        update_query = """
        UPDATE devdw.offices d
SET
    city = s.city,
    phone = s.phone,
    addressline1 = s.addressline1,
    addressline2 = s.addressline2,
    state = s.state,
    country = s.country,
    postalcode = s.postalcode,
    territory = s.territory,
    src_update_timestamp = s.update_timestamp,
    etl_batch_no = %s, -- Placeholder for ETL batch number
    etl_batch_date = %s, -- Placeholder for ETL batch date
    dw_update_timestamp = CURRENT_TIMESTAMP -- Replace with current timestamp
FROM devstage.offices s
WHERE d.officecode = s.officecode;
        """
      

        # Insert new records into the OFFICES table from the devstage schema
        insert_query = """
       INSERT INTO devdw.offices (
    officecode, city, phone, addressline1, addressline2, state,
    country, postalcode, territory, src_create_timestamp,
    src_update_timestamp, etl_batch_no, etl_batch_date
)
SELECT
    s.officecode, s.city, s.phone, s.addressline1, s.addressline2, s.state,
    s.country, s.postalcode, s.territory, s.create_timestamp,
    s.update_timestamp, %s, %s -- Placeholders for ETL batch number and date
FROM devstage.offices s
LEFT JOIN devdw.offices d ON s.officecode = d.officecode
WHERE d.officecode IS NULL;


        """
        
    
        cursor.execute(insert_query,(etl_batch_no, etl_batch_date))
        cursor.execute(update_query,(etl_batch_no, etl_batch_date))


        # Commit the transaction
        connection.commit()
        print(f"Data successfully updated and inserted into devdw.offices.")

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
