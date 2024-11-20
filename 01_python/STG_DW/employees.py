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
#     """Load data from devstage to devdw and update timestamps in the employees table."""
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


#         insert_query = """
#             INSERT INTO devdw.EMPLOYEES (
#             EMPLOYEENUMBER, 
#             LASTNAME, 
#             FIRSTNAME, 
#             EXTENSION, 
#             EMAIL, 
#             OFFICECODE, 
#             REPORTSTO, 
#             JOBTITLE, 
#             SRC_CREATE_TIMESTAMP, 
#             SRC_UPDATE_TIMESTAMP,
#             DW_CREATE_TIMESTAMP,
#             DW_UPDATE_TIMESTAMP,
#             DW_REPORTING_EMPLOYEE_ID, 
#             DW_OFFICE_ID,
#             ETL_BATCH_NO,
#             ETL_BATCH_DATE
#         )
#         SELECT 
#             A.EMPLOYEENUMBER,
#             A.LASTNAME,
#             A.FIRSTNAME,
#             A.EXTENSION,
#             A.EMAIL,
#             A.OFFICECODE,
#             A.REPORTSTO,
#             A.JOBTITLE,
#             A.CREATE_TIMESTAMP AS SRC_CREATE_TIMESTAMP,
#             A.UPDATE_TIMESTAMP AS SRC_UPDATE_TIMESTAMP,
#             CURRENT_TIMESTAMP AS DW_CREATE_TIMESTAMP,
#             CURRENT_TIMESTAMP AS DW_UPDATE_TIMESTAMP,
#             C.DW_EMPLOYEE_ID AS DW_REPORTING_EMPLOYEE_ID, 
#             D.DW_OFFICE_ID AS DW_OFFICE_ID ,
#             %s,
#             %s
#         FROM 
#             devstage.EMPLOYEES A
#         LEFT JOIN 
#             devdw.EMPLOYEES B ON A.EMPLOYEENUMBER = B.EMPLOYEENUMBER
#         LEFT JOIN 
#             devdw.EMPLOYEES C ON A.REPORTSTO = C.EMPLOYEENUMBER       
#         LEFT JOIN 
#             devdw.OFFICES D ON A.OFFICECODE = D.OFFICECODE          
#         WHERE 
#             B.EMPLOYEENUMBER IS NULL;
#         """
        
#         update_query = """
#         UPDATE devdw.EMPLOYEES B
#         SET 
#             LASTNAME = A.LASTNAME,
#             FIRSTNAME = A.FIRSTNAME,
#             EXTENSION = A.EXTENSION,
#             EMAIL = A.EMAIL,
#             OFFICECODE = A.OFFICECODE,
#             REPORTSTO = A.REPORTSTO,
#             JOBTITLE = A.JOBTITLE,
#             SRC_UPDATE_TIMESTAMP = A.UPDATE_TIMESTAMP,
#             DW_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP,
#             DW_REPORTING_EMPLOYEE_ID = C.DW_EMPLOYEE_ID,
#             DW_OFFICE_ID = D.DW_OFFICE_ID,
#             ETL_BATCH_NO=%s,
#             ETL_BATCH_DATE=%s   
#             FROM devstage.EMPLOYEES A 
#             LEFT JOIN devdw.EMPLOYEES C ON A.REPORTSTO = C.EMPLOYEENUMBER
#             LEFT JOIN devdw.OFFICES D ON A.OFFICECODE = D.OFFICECODE 
#             WHERE B.EMPLOYEENUMBER=A.EMPLOYEENUMBER;

#                 """
#         cursor.execute(update_query,(etl_batch_no, etl_batch_date))
#         cursor.execute(insert_query,(etl_batch_no, etl_batch_date))

        

#         # Commit the transaction
#         connection.commit()
#         print(F"Data successfully updated and inserted into devdw.employees.")

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
    """Load data from devstage to devdw and update timestamps in the employees table."""
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


        insert_query = """
           INSERT INTO devdw.employees (
    employeenumber, lastname, firstname, extension, email, officecode,
    reportsto, jobtitle, src_create_timestamp, src_update_timestamp,
    dw_office_id, etl_batch_no, etl_batch_date
)
SELECT
    s.employeenumber, s.lastname, s.firstname, s.extension, s.email,
    s.officecode, s.reportsto, s.jobtitle, s.create_timestamp,
    s.update_timestamp, o.dw_office_id, %s, %s
FROM devstage.employees s
LEFT JOIN devdw.offices o ON s.officecode = o.officecode
LEFT JOIN devdw.employees d ON s.employeenumber = d.employeenumber
WHERE d.employeenumber IS NULL;
        """
        
        update_query = """
       UPDATE devdw.employees d
SET
    lastname = s.lastname,
    firstname = s.firstname,
    extension = s.extension,
    email = s.email,
    officecode = s.officecode,
    reportsto = s.reportsto,
    jobtitle = s.jobtitle,
    src_update_timestamp = s.update_timestamp,
    dw_update_timestamp = CURRENT_TIMESTAMP, -- Replace with the current timestamp
    dw_office_id = o.dw_office_id,
    dw_reporting_employee_id = e.dw_employee_id,
    etl_batch_no = %s, -- Placeholder for ETL batch number
    etl_batch_date = %s -- Placeholder for ETL batch date
FROM devstage.employees s
LEFT JOIN devdw.offices o ON s.officecode = o.officecode
LEFT JOIN devdw.employees e ON s.reportsto = e.employeenumber
WHERE d.employeenumber = s.employeenumber;

                """
        cursor.execute(update_query,(etl_batch_no, etl_batch_date))
        cursor.execute(insert_query,(etl_batch_no, etl_batch_date))

        

        # Commit the transaction
        connection.commit()
        print(F"Data successfully updated and inserted into devdw.employees.")

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
