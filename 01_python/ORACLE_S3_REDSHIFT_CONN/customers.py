# # # import os
# # # import sys
# # # import boto3
# # # import oracledb
# # # import psycopg2
# # # from io import BytesIO
# # # sys.path.append(os.path.abspath(".."))
# # # from config import username, password, dsn, bucket, client_path, redshift_username, redshift_password, redshift_dsn

# # # # AWS S3 Setup
# # # client = boto3.client('s3')

# # # # Initialize Oracle Client
# # # oracledb.init_oracle_client(lib_dir=client_path)

# # # def get_etl_batch_date():
# # #     """Fetch the ETL batch date from Redshift."""
# # #     connection = None
# # #     cursor = None
# # #     try:
# # #         # Connect to Redshift using the details in the redshift_dsn dictionary
# # #         connection = psycopg2.connect(
# # #             dbname=redshift_dsn['dbname'],
# # #             user=redshift_username,
# # #             password=redshift_password,
# # #             host=redshift_dsn['host'],
# # #             port=redshift_dsn['port']
# # #         )
        
# # #         cursor = connection.cursor()
# # #         etl_batch_date_query = "SELECT etl_batch_date FROM metadata.batch_control"
# # #         cursor.execute(etl_batch_date_query)
# # #         result = cursor.fetchone()
        
# # #         if result is None:
# # #             print("No ETL batch date found in the batch_control table.")
# # #             return None
        
# # #         return result[0]  # Return the first column of the result
# # #     except Exception as e:
# # #         print(f"Error fetching batch date from Redshift: {e}")
# # #         return None
# # #     finally:
# # #         if cursor:
# # #             cursor.close()
# # #         if connection:
# # #             connection.close()

# # # def export_table(table_name, s3_folder):
# # #     """Export table data from Oracle to S3."""
# # #     # Get the ETL batch date from Redshift
# # #     etl_batch_date = get_etl_batch_date()
    
# # #     if etl_batch_date is None:
# # #         return  # Exit if we couldn't get the batch date
    
# # #     connection = None
# # #     cursor = None
# # #     try:
# # #         # Connect to Oracle
# # #         connection = oracledb.connect(user=username, password=password, dsn=dsn)
        
# # #         query = f"""
# # #         SELECT  * FROM {table_name}@dblink
# # #         WHERE UPDATE_TIMESTAMP >= :etl_batch_date 
# # #         """
        
# # #         # Use a cursor to execute the SQL query
# # #         cursor = connection.cursor()
# # #         cursor.execute(query, {'etl_batch_date': etl_batch_date})

# # #         # Fetch all results
# # #         data = cursor.fetchall()
# # #         columns = [desc[0] for desc in cursor.description]
        
# # #         if not data:
# # #             print(f"No data found for {table_name} for the specified update_timestamp.")
# # #             return

# # #         # Create a CSV from the fetched data
# # #         csv_buffer = BytesIO()
# # #         header = ','.join(columns) + '\n'
# # #         csv_buffer.write(header.encode())
        
# # #         for row in data:
# # #             csv_buffer.write(','.join(map(str, row)).encode() + b'\n')
        
# # #         csv_buffer.seek(0)

# # #         s3_key = f"{s3_folder}/{etl_batch_date.strftime('%Y-%m-%d')}/{table_name}.csv"
        
# # #         # Upload to S3
# # #         client.upload_fileobj(csv_buffer, bucket, s3_key)
# # #         print(f"Uploaded {table_name} to s3://{bucket}/{s3_key}")
# # #     except Exception as e:
# # #         print(f"Error exporting {table_name}: {e}")
# # #     finally:
# # #         if cursor:
# # #             cursor.close()
# # #         if connection:
# # #             connection.close()

# # # export_table('CUSTOMERS', 'CUSTOMERS')



# # import os
# # import sys
# # import boto3
# # import oracledb
# # import psycopg2
# # from io import BytesIO
# # sys.path.append(os.path.abspath(".."))
# # from config import username, password, dsn, bucket, client_path, redshift_username, redshift_password, redshift_dsn

# # # AWS S3 Setup
# # client = boto3.client('s3')

# # # Initialize Oracle Client
# # oracledb.init_oracle_client(lib_dir=client_path)

# # def get_etl_batch_date():
# #     """Fetch the ETL batch date from Redshift."""
# #     connection = None
# #     cursor = None
# #     try:
# #         # Connect to Redshift using the details in the redshift_dsn dictionary
# #         connection = psycopg2.connect(
# #             dbname=redshift_dsn['dbname'],
# #             user=redshift_username,
# #             password=redshift_password,
# #             host=redshift_dsn['host'],
# #             port=redshift_dsn['port']
# #         )
        
# #         cursor = connection.cursor()
# #         etl_batch_date_query = "SELECT etl_batch_date FROM metadata.batch_control"
# #         cursor.execute(etl_batch_date_query)
# #         result = cursor.fetchone()
        
# #         if result is None:
# #             print("No ETL batch date found in the batch_control table.")
# #             return None
        
# #         return result[0]  # Return the first column of the result
# #     except Exception as e:
# #         print(f"Error fetching batch date from Redshift: {e}")
# #         return None
# #     finally:
# #         if cursor:
# #             cursor.close()
# #         if connection:
# #             connection.close()

# # def export_table(table_name, s3_folder):
# #     """Export table data from Oracle to S3."""
# #     # Get the ETL batch date from Redshift
# #     etl_batch_date = get_etl_batch_date()
    
# #     if etl_batch_date is None:
# #         return  # Exit if we couldn't get the batch date
    
# #     connection = None
# #     cursor = None
# #     try:
# #         # Connect to Oracle
# #         connection = oracledb.connect(user=username, password=password, dsn=dsn)
        
# #         query = f"""
# #             SELECT  CUSTOMERNUMBER, CUSTOMERNAME, CONTACTLASTNAME, CONTACTFIRSTNAME,
# #             PHONE, ADDRESSLINE1, ADDRESSLINE2, CITY, STATE, POSTALCODE,
# #             COUNTRY, SALESREPEMPLOYEENUMBER, CREDITLIMIT, CREATE_TIMESTAMP,
# #             UPDATE_TIMESTAMP FROM {table_name}@dblink
# #             WHERE UPDATE_TIMESTAMP >= :etl_batch_date 
# #             """
        
# #         # Use a cursor to execute the SQL query
# #         cursor = connection.cursor()
# #         cursor.execute(query, {'etl_batch_date': etl_batch_date})

# #         # Fetch all results
# #         data = cursor.fetchall()
# #         columns = [desc[0] for desc in cursor.description]
        
# #         # Create a CSV from the fetched data (with headers)
# #         csv_buffer = BytesIO()
# #         header = ','.join(columns) + '\n'
# #         csv_buffer.write(header.encode())
        
# #         if data:
# #             for row in data:
# #                 csv_buffer.write(','.join(map(str, row)).encode() + b'\n')
        
# #         # Reset buffer position to the beginning
# #         csv_buffer.seek(0)

# #         s3_key = f"{s3_folder}/{etl_batch_date.strftime('%Y-%m-%d')}/{table_name}.csv"
        
# #         # Upload to S3 (will contain headers only if no data is present)
# #         client.upload_fileobj(csv_buffer, bucket, s3_key)
# #         print(f"Uploaded {table_name} to s3://{bucket}/{s3_key}")
        
# #     except Exception as e:
# #         print(f"Error exporting {table_name}: {e}")
# #     finally:
# #         if cursor:
# #             cursor.close()
# #         if connection:
# #             connection.close()

# # export_table('CUSTOMERS', 'CUSTOMERS')

# import os
# import csv
# import boto3
# import oracledb
# import redshift_connector
# from datetime import datetime
# from dotenv import load_dotenv
# from botocore.exceptions import NoCredentialsError

# # Load environment variables
# load_dotenv()

# # Configuration Section
# table_name = "CUSTOMERS"
# dblink_name = "dblink"  # Set the database link name here

# # Columns to fetch from the Oracle table
# columns = [
#     'CUSTOMERNUMBER', 'CUSTOMERNAME', 'CONTACTLASTNAME', 'CONTACTFIRSTNAME', 
#     'PHONE', 'ADDRESSLINE1', 'ADDRESSLINE2', 'CITY', 'STATE', 'POSTALCODE',
#     'COUNTRY', 'SALESREPEMPLOYEENUMBER', 'CREDITLIMIT', 'CREATE_TIMESTAMP', 
#     'UPDATE_TIMESTAMP'
# ]

# # Oracle and S3 configuration
# oracle_client_path = os.getenv("ORACLE_CLIENT_PATH")
# oracledb.init_oracle_client(lib_dir=oracle_client_path)

# # Redshift configuration
# redshift_host = os.getenv("REDSHIFT_HOST")
# redshift_port = os.getenv("REDSHIFT_PORT")
# redshift_db = os.getenv("REDSHIFT_DBNAME")
# redshift_user = os.getenv("REDSHIFT_USER")
# redshift_password = os.getenv("REDSHIFT_PASSWORD")

# # AWS S3 configuration
# bucket_name = os.getenv("AWS_BUCKET_NAME")
# backup_suffix = datetime.now().strftime('%Y%m%d%H%M%S')

# s3 = boto3.client(
#     's3',
#     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
#     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
#     region_name=os.getenv("AWS_REGION")
# )

# def get_oracle_connection():
#     host = os.getenv("ORACLE_HOST")
#     port = os.getenv("ORACLE_PORT")
#     service_name = os.getenv("ORACLE_SERVICE_NAME")
#     username = os.getenv("ORACLE_USERNAME")
#     password = os.getenv("ORACLE_PASSWORD")

#     dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))(CONNECT_DATA=(SERVICE_NAME={service_name})))"
#     return oracledb.connect(user=username, password=password, dsn=dsn)

# def get_redshift_connection():
#     return redshift_connector.connect(
#         host=redshift_host,
#         port=redshift_port,
#         database=redshift_db,
#         user=redshift_user,
#         password=redshift_password
#     )

# def get_etl_batch_date():
#     """Fetches the latest etl_batch_date from metadata.batch_control table in Redshift."""
#     connection = get_redshift_connection()
#     cursor = connection.cursor()
#     query = "SELECT MAX(etl_batch_date) FROM metadata.batch_control"
#     try:
#         cursor.execute(query)
#         etl_batch_date = cursor.fetchone()[0]
#         if etl_batch_date:
#             return etl_batch_date.strftime('%Y%m%d')  # Format as needed for S3 path
#         else:
#             raise ValueError("No etl_batch_date found in batch_control.")
#     except redshift_connector.Error as e:
#         print(f"Error fetching etl_batch_date: {e}")
#         raise
#     finally:
#         cursor.close()
#         connection.close()

# def fetch_and_upload_table():
#     etl_batch_date = get_etl_batch_date()
#     temp_dir = "./temp"
#     local_dir = os.path.join(temp_dir, table_name)
#     os.makedirs(local_dir, exist_ok=True)
#     file_name = f"{table_name}.csv"
#     local_path = os.path.join(local_dir, file_name)

#     query = f"""
#     SELECT {', '.join(columns)}
#     FROM {table_name}@{dblink_name}
#     WHERE UPDATE_TIMESTAMP >= TO_DATE('{etl_batch_date}', 'YYYYMMDD')
#     """
    
#     connection = get_oracle_connection()
#     cursor = connection.cursor()

#     try:
#         cursor.execute(query)
#         with open(local_path, 'w', newline='') as csvfile:
#             writer = csv.writer(csvfile)
#             writer.writerow([desc[0] for desc in cursor.description])
#             writer.writerows(cursor)

#         # Construct S3 path with table_name and etl_batch_date subfolder
#         s3_key_prefix = f"{table_name}/{etl_batch_date}/{file_name}"

#         # Backup if file exists in S3
#         try:
#             s3.head_object(Bucket=bucket_name, Key=s3_key_prefix)
#             backup_key = f"{table_name}/{etl_batch_date}/{table_name}_{backup_suffix}.csv"
#             s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': s3_key_prefix}, Key=backup_key)
#             s3.delete_object(Bucket=bucket_name, Key=s3_key_prefix)
#             print(f"Backed up existing file as {backup_key}")
#         except s3.exceptions.ClientError:
#             pass  # No existing file, no backup needed

#         # Upload new file
#         s3.upload_file(local_path, bucket_name, s3_key_prefix)
#         print(f"Uploaded {file_name} to S3 at {s3_key_prefix}")

#     except oracledb.DatabaseError as e:
#         print(f"Error executing query for {table_name}@{dblink_name}: {e}")
#     finally:
#         cursor.close()
#         connection.close()
#         if os.path.exists(local_path):
#             os.remove(local_path)
#         if os.path.exists(local_dir):
#             os.rmdir(local_dir)  # Remove the local directory if it's empty

# if __name__ == "__main__":
#     try:
#         fetch_and_upload_table()
#     except NoCredentialsError:
#         print("AWS credentials not available.")
#     except Exception as e:
#         print(f"An error occurred: {e}")

import oracledb
import boto3
import csv
import os
import redshift_connector
from io import StringIO

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Initialize Oracle client libraries
oracle_client_path = os.getenv("ORACLE_CLIENT_PATH")
oracledb.init_oracle_client(lib_dir=oracle_client_path)

# Connect to Oracle database
oracle_connection = oracledb.connect(
    host=os.getenv("ORACLE_HOST"),
    port=os.getenv("ORACLE_PORT"),
    service_name=os.getenv("ORACLE_SERVICE_NAME"),
    user=os.getenv("ORACLE_USERNAME"),
    password=os.getenv("ORACLE_PASSWORD")
)

# Connect to S3 bucket
s3 = boto3.client('s3')

# Table-specific details for CUSTOMERS
db = "dblink"
table_name = "CUSTOMERS"
columns = (
    "CUSTOMERNUMBER, CUSTOMERNAME, CONTACTLASTNAME, CONTACTFIRSTNAME, PHONE, "
    "ADDRESSLINE1, ADDRESSLINE2, CITY, STATE, POSTALCODE, COUNTRY, "
    "SALESREPEMPLOYEENUMBER, CREDITLIMIT, CREATE_TIMESTAMP, UPDATE_TIMESTAMP"
)

# Redshift connection
redshift_host = os.getenv("REDSHIFT_HOST")
redshift_port = os.getenv("REDSHIFT_PORT")
redshift_db = os.getenv("REDSHIFT_DBNAME")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
redshift_connection = redshift_connector.connect(
    host=redshift_host,
    port=redshift_port,
    database=redshift_db,
    user=redshift_user,
    password=redshift_password
)

# Fetch the latest ETL batch date from Redshift
redshift_cursor = redshift_connection.cursor()
redshift_cursor.execute("SELECT etl_batch_date FROM metadata.batch_control")
result = redshift_cursor.fetchall()
batch_date = result[0][0]
redshift_cursor.close()

# Query the Oracle database for updated customer data
oracle_cursor = oracle_connection.cursor()
query = (
    f"SELECT {columns} FROM {table_name}@{db} "
    f"WHERE UPDATE_TIMESTAMP >= TO_DATE('{batch_date}', 'YYYY-MM-DD')"
)
oracle_cursor.execute(query)
results = oracle_cursor.fetchall()

# Write data to an in-memory CSV file
f = StringIO()
writer = csv.writer(f)
writer.writerow(desc[0] for desc in oracle_cursor.description)  # Write headers
writer.writerows(results)  # Write rows
f.seek(0)

# Upload the CSV to the S3 bucket
print(f"Loading {table_name} data to S3...")
s3.put_object(
    Bucket=os.getenv("aws_bucket_name"),
    Key=f"{table_name}/{batch_date}/{table_name}.csv",
    Body=f.getvalue()
)
print(
    f"{table_name} data has been uploaded to the {batch_date} folder in "
    f"the {os.getenv('aws_bucket_name')} S3 bucket."
)

# Close connections
oracle_cursor.close()
oracle_connection.close()
redshift_connection.close()
