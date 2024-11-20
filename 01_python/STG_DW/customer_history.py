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
    """Load data from devstage to devdw and update timestamps in the customer_history table."""
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

        # Update DW_UPDATE_TIMESTAMP for all records in the customer_history table
        update_query = """
        UPDATE devdw.CUSTOMER_HISTORY B
        SET 
            EFFECTIVE_TO_DATE = DATEADD(DAY, -1, %s),
            DW_ACTIVE_RECORD_IND = 0,
            UPDATE_ETL_BATCH_NO = %s,
            UPDATE_ETL_BATCH_DATE = %s,
            DW_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP
        FROM 
            devdw.CUSTOMERS A
        WHERE 
            A.DW_CUSTOMER_ID = B.DW_CUSTOMER_ID 
            AND B.DW_ACTIVE_RECORD_IND = 1 
            AND A.CREDITLIMIT != B.CREDITLIMIT;

        


        """
        cursor.execute(update_query, (etl_batch_date, etl_batch_no, etl_batch_date))
        
        # Insert new records into the customer_history table from the devstage schema
        insert_query = """
        INSERT INTO devdw.CUSTOMER_HISTORY (
            DW_CUSTOMER_ID,
            CREDITLIMIT,
            EFFECTIVE_FROM_DATE,
            DW_ACTIVE_RECORD_IND,
            DW_CREATE_TIMESTAMP,
            DW_UPDATE_TIMESTAMP,
            CREATE_ETL_BATCH_NO,
            CREATE_ETL_BATCH_DATE
        )
        SELECT 
            A.DW_CUSTOMER_ID,
            A.CREDITLIMIT,
            %s AS EFFECTIVE_FROM_DATE,
            1 AS DW_ACTIVE_RECORD_IND,
            CURRENT_TIMESTAMP AS DW_CREATE_TIMESTAMP,
            CURRENT_TIMESTAMP AS DW_UPDATE_TIMESTAMP,
            %s AS CREATE_ETL_BATCH_NO,
            %s AS CREATE_ETL_BATCH_DATE
        FROM 
            devdw.CUSTOMERS A
        LEFT JOIN 
            devdw.CUSTOMER_HISTORY B
        ON 
            A.DW_CUSTOMER_ID = B.DW_CUSTOMER_ID
        WHERE 
            B.DW_CUSTOMER_ID IS NULL
            OR B.UPDATE_ETL_BATCH_DATE = %s;

        """
        cursor.execute(insert_query, (etl_batch_date, etl_batch_no, etl_batch_date, etl_batch_date))

        # Commit the transaction
        connection.commit()
        print("Data successfully updated and inserted into devdw.customer_history.")

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
