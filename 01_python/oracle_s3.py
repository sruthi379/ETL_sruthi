import os
import sys
import subprocess
import concurrent.futures
import oracledb
import psycopg2
from datetime import datetime

# Add the path to the parent directory for config
sys.path.append(os.path.abspath(".."))
from config import redshift_username, redshift_password, redshift_dsn



# List of ETL scripts to be run in parallel
scripts = [
    'ORACLE_S3_REDSHIFT_CONN/customers.py',
    'ORACLE_S3_REDSHIFT_CONN/employees.py',
    'ORACLE_S3_REDSHIFT_CONN/offices.py',
    'ORACLE_S3_REDSHIFT_CONN/orderdetails.py',
    'ORACLE_S3_REDSHIFT_CONN/orders.py',
    'ORACLE_S3_REDSHIFT_CONN/payments.py',
    'ORACLE_S3_REDSHIFT_CONN/productlines.py',
    'ORACLE_S3_REDSHIFT_CONN/products.py'
]

# Function to log the start of each batch in Redshift
def log_batch_start(etl_batch_no, etl_batch_date):
    try:
        connection = psycopg2.connect(
            dbname=redshift_dsn['dbname'],
            user=redshift_username,
            password=redshift_password,
            host=redshift_dsn['host'],
            port=redshift_dsn['port']
        )
        
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO metadata.batch_control_log (
                etl_batch_no,
                etl_batch_date,
                etl_batch_status,
                etl_batch_start_time
            ) VALUES (
                %s, %s, 'O', CURRENT_TIMESTAMP
            )
            """,
            (etl_batch_no, etl_batch_date)
        )
        connection.commit()
        print(f"Batch start logged: {etl_batch_no}")
    except Exception as e:
        print(f"Error logging batch start: {e}")

# Function to log the completion or failure of each batch in Redshift
def log_batch_end(etl_batch_no, status='C'):
    try:
        connection = psycopg2.connect(
            dbname=redshift_dsn['dbname'],
            user=redshift_username,
            password=redshift_password,
            host=redshift_dsn['host'],
            port=redshift_dsn['port']
        )
        
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE metadata.batch_control_log
            SET etl_batch_status = %s, etl_batch_end_time = CURRENT_TIMESTAMP
            WHERE etl_batch_no = %s
            """,
            (status, etl_batch_no)
        )
        connection.commit()
        print(f"Batch end logged with status {status}: {etl_batch_no}")
    except Exception as e:
        print(f"Error logging batch end: {e}")

# Function to execute each ETL script and handle logging
def run_script(script_name, etl_batch_no, etl_batch_date):
    try:
        # Run the script and check if it completes successfully
        subprocess.run(['python', script_name], check=True)
        print(f"{script_name} completed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e}")
        return False

# Main execution
if __name__ == "__main__":
    try:
        connection = psycopg2.connect(
            dbname=redshift_dsn['dbname'],
            user=redshift_username,
            password=redshift_password,
            host=redshift_dsn['host'],
            port=redshift_dsn['port']
        )
        
        cursor = connection.cursor()
        cursor.execute(
                """
                SELECT etl_batch_no, etl_batch_date 
                FROM metadata.batch_control 
                """
            )
        result = cursor.fetchone()
            
        if result is None:
                print("No batch information found. Please ensure a batch has been logged.")
                sys.exit(1)  # Exit if no batch number is found

        etl_batch_no, etl_batch_date = result
        print(f"Using Batch ID: {etl_batch_no} with date: {etl_batch_date}")

    except Exception as e:
        print(f"Error fetching batch information: {e}")
        sys.exit(1)

    log_batch_start(etl_batch_no, etl_batch_date)  # Log the start of the batch load

    # Run all ETL scripts in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(run_script, script, etl_batch_no, etl_batch_date): script for script in scripts}
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    # If all scripts were successful, log completion as 'C', else 'F'
    if all(results):
        log_batch_end(etl_batch_no, status='C')
    else:
        log_batch_end(etl_batch_no, status='F')

    print("All scripts have finished executing.")

# Close the connection at the end
connection.close()
