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
# #         SELECT ORDERNUMBER, PRODUCTCODE, QUANTITYORDERED, PRICEEACH, ORDERLINENUMBER
# #         FROM {table_name}@dblink
# #         WHERE UPDATE_TIMESTAMP >= :etl_batch_date
# #         """
        
# #         # Use a cursor to execute the SQL query
# #         cursor = connection.cursor()
# #         cursor.execute(query, {'etl_batch_date': etl_batch_date})

# #         # Fetch all results
# #         data = cursor.fetchall()
# #         columns = [desc[0] for desc in cursor.description]
        
# #         if not data:
# #             print(f"No data found for {table_name} for the specified update_timestamp.")
# #             return

# #         # Create a CSV from the fetched data
# #         csv_buffer = BytesIO()
# #         header = ','.join(columns) + '\n'
# #         csv_buffer.write(header.encode())
        
# #         for row in data:
# #             csv_buffer.write(','.join(map(str, row)).encode() + b'\n')
        
# #         csv_buffer.seek(0)

# #         s3_key = f"{s3_folder}/{etl_batch_date.strftime('%Y-%m-%d')}/{table_name}.csv"
        
# #         # Upload to S3
# #         client.upload_fileobj(csv_buffer, bucket, s3_key)
# #         print(f"Uploaded {table_name} to s3://{bucket}/{s3_key}")
# #     except Exception as e:
# #         print(f"Error exporting {table_name}: {e}")
# #     finally:
# #         if cursor:
# #             cursor.close()
# #         if connection:
# #             connection.close()

# # export_table('ORDERDETAILS', 'ORDERDETAILS')






# import os
# import sys
# import boto3
# import oracledb
# import psycopg2
# from io import BytesIO
# sys.path.append(os.path.abspath(".."))
# from config import username, password, dsn, bucket, client_path, redshift_username, redshift_password, redshift_dsn

# # AWS S3 Setup
# client = boto3.client('s3')

# # Initialize Oracle Client
# oracledb.init_oracle_client(lib_dir=client_path)

# def get_etl_batch_date():
#     """Fetch the ETL batch date from Redshift."""
#     connection = None
#     cursor = None
#     try:
#         # Connect to Redshift using the details in the redshift_dsn dictionary
#         connection = psycopg2.connect(
#             dbname=redshift_dsn['dbname'],
#             user=redshift_username,
#             password=redshift_password,
#             host=redshift_dsn['host'],
#             port=redshift_dsn['port']
#         )
        
#         cursor = connection.cursor()
#         etl_batch_date_query = "SELECT etl_batch_date FROM metadata.batch_control"
#         cursor.execute(etl_batch_date_query)
#         result = cursor.fetchone()
        
#         if result is None:
#             print("No ETL batch date found in the batch_control table.")
#             return None
        
#         return result[0]  # Return the first column of the result
#     except Exception as e:
#         print(f"Error fetching batch date from Redshift: {e}")
#         return None
#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()

# def export_table(table_name, s3_folder):
#     """Export table data from Oracle to S3."""
#     # Get the ETL batch date from Redshift
#     etl_batch_date = get_etl_batch_date()
    
#     if etl_batch_date is None:
#         return  # Exit if we couldn't get the batch date
    
#     connection = None
#     cursor = None
#     try:
#         # Connect to Oracle
#         connection = oracledb.connect(user=username, password=password, dsn=dsn)
        
#         query = f"""
#         SELECT ORDERNUMBER, PRODUCTCODE, QUANTITYORDERED, PRICEEACH, ORDERLINENUMBER, CREATE_TIMESTAMP, UPDATE_TIMESTAMP
#         FROM {table_name}@dblink
#         WHERE UPDATE_TIMESTAMP >= :etl_batch_date
#         """
        
#         # Use a cursor to execute the SQL query
#         cursor = connection.cursor()
#         cursor.execute(query, {'etl_batch_date': etl_batch_date})

#         # Fetch all results
#         data = cursor.fetchall()
#         columns = [desc[0] for desc in cursor.description]
        
#         # Create a CSV from the fetched data (with headers)
#         csv_buffer = BytesIO()
#         header = ','.join(columns) + '\n'
#         csv_buffer.write(header.encode())
        
#         if data:
#             for row in data:
#                 csv_buffer.write(','.join(map(str, row)).encode() + b'\n')
        
#         # Reset buffer position to the beginning
#         csv_buffer.seek(0)

#         s3_key = f"{s3_folder}/{etl_batch_date.strftime('%Y-%m-%d')}/{table_name}.csv"
        
#         # Upload to S3 (will contain headers only if no data is present)
#         client.upload_fileobj(csv_buffer, bucket, s3_key)
#         print(f"Uploaded {table_name} to s3://{bucket}/{s3_key}")
        
#     except Exception as e:
#         print(f"Error exporting {table_name}: {e}")
#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()

# export_table('ORDERDETAILS', 'ORDERDETAILS')

import oracledb
import boto3
import csv
import os
import redshift_connector
from io import StringIO

#load environment variables 
from dotenv import load_dotenv
load_dotenv()

#get oracle clinent libraries
oracle_client_path = os.getenv("ORACLE_CLIENT_PATH")
oracledb.init_oracle_client(lib_dir=oracle_client_path)

#connecting to oracle db
connection = oracledb.connect(
     host = os.getenv("ORACLE_HOST"),
     port = os.getenv("ORACLE_PORT"),
     service_name = os.getenv("ORACLE_SERVICE_NAME"),
     user = os.getenv("ORACLE_USERNAME"),
     password = os.getenv("ORACLE_PASSWORD")
)
#connecting to s3 bucket
s3 = boto3.client('s3')


db="dblink"

tables = 'ORDERDETAILS'
headers = "orderNumber,productCode,quantityOrdered,priceEach,orderLineNumber,create_timestamp,update_timestamp"

redshift_host = os.getenv("REDSHIFT_HOST")
redshift_port = os.getenv("REDSHIFT_PORT")
redshift_db = os.getenv("REDSHIFT_DBNAME")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
conn = redshift_connector.connect(
        host=redshift_host,
        port=redshift_port,
        database=redshift_db,
        user=redshift_user,
        password=redshift_password                           
)



cursor = conn.cursor()
cursor.execute("SELECT etl_batch_date FROM metadata.batch_control")
result = cursor.fetchall()
batch_date = result[0][0]
cursor = connection.cursor()
query=f'SELECT {headers} FROM {tables}@{db} WHERE UPDATE_TIMESTAMP >= TO_DATE(\'{batch_date}\', \'YYYY-MM-DD\')'
cursor.execute(query)
results =  cursor.fetchall()
#creating  temp memory
f= StringIO()
writer=csv.writer(f)
writer.writerow(desc[0] for desc in cursor.description)
writer.writerows(results)
f.seek(0)
# creating/updating  tables.csvs in s3
print(f'loading {tables} data.......')
s3.put_object(
    Bucket=os.getenv('aws_bucket_name'),
    Key=f'{tables}/{batch_date}/{tables}.csv',
    Body=f.getvalue()
        )
print(f'{tables} is created in {batch_date} folder {os.getenv("aws_bucket_name")} in s3 bucket' )
cursor.close()
connection.close()