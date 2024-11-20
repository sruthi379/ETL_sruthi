import os
import sys
import boto3
import psycopg2
sys.path.append(os.path.abspath(".."))
from config import redshift_username, redshift_password, redshift_dsn, bucket
from dotenv import load_dotenv, dotenv_values
load_dotenv()
# AWS S3 and Redshift Setup
client = boto3.client('s3')

def get_etl_batch_date():
    """Fetch the ETL batch date from Redshift."""
    connection = None
    cursor = None
    try:
        # Connect to Redshift using the details in the redshift_dsn dictionary
        connection = psycopg2.connect(
            dbname=redshift_dsn['dbname'],
            user=redshift_username,
            password=redshift_password,
            host=redshift_dsn['host'],
            port=redshift_dsn['port']
        )
        
        cursor = connection.cursor()
        etl_batch_date_query = "SELECT etl_batch_date FROM metadata.batch_control"
        cursor.execute(etl_batch_date_query)
        result = cursor.fetchone()
        
        if result is None:
            print("No ETL batch date found in the batch_control table.")
            return None
        
        return result[0]  # Return the first column of the result
    except Exception as e:
        print(f"Error fetching batch date from Redshift: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def load_data_to_redshift(table_name, s3_folder, etl_batch_date):
    """Load data from S3 to Redshift table."""
    connection = None
    cursor = None
    try:
        # Connect to Redshift using the details in the redshift_dsn dictionary
        connection = psycopg2.connect(
            dbname=redshift_dsn['dbname'],
            user=redshift_username,
            password=redshift_password,
            host=redshift_dsn['host'],
            port=redshift_dsn['port']
        )
        cursor = connection.cursor()
        
        # Define the S3 file path and COPY command
        s3_key = f"s3://{bucket}/{s3_folder}/{etl_batch_date.strftime('%Y-%m-%d')}/{table_name}.csv"
        copy_command = f"""
        COPY devstage.{table_name}
        FROM '{s3_key}'
        IAM_ROLE '{os.getenv("REDSHIFT_IAM_ROLE")}'  
        FORMAT AS CSV
        IGNOREHEADER 1
        DELIMITER ','
        DATEFORMAT 'auto'
        ACCEPTINVCHARS AS '?'
        FILLRECORD
        IGNOREBLANKLINES
        TRIMBLANKS
        EMPTYASNULL;
        """
        
        # Execute the COPY command to load data from S3 to Redshift
        cursor.execute(copy_command)
        connection.commit()
        print(f"Data loaded successfully into devstage.{table_name} from {s3_key}")

    except Exception as e:
        print(f"Error loading data to Redshift: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Fetch the ETL batch date before calling the load function
etl_batch_date = get_etl_batch_date()
if etl_batch_date:
    load_data_to_redshift('ORDERDETAILS', 'ORDERDETAILS', etl_batch_date)
else:
    print("ETL batch date not found. Exiting.")