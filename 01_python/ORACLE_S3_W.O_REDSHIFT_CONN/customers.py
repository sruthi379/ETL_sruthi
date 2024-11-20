import os
import sys
import pandas as pd
import boto3
from sqlalchemy import create_engine
import oracledb
from io import BytesIO

# Add the path to the parent directory
sys.path.append(os.path.abspath(".."))
from config import username, password, dsn, bucket, client_path

# AWS S3 Setup
client = boto3.client('s3')

# Initialize Oracle Client
oracledb.init_oracle_client(lib_dir=client_path)

# Create a SQLAlchemy engine
engine = create_engine(f'oracle+oracledb://{username}:{password}@{dsn}')

def export_table(table_name, s3_folder):
    connection = engine.connect()
    try:
        etl_batch_date_query = "SELECT etl_batch_date FROM H24SRUTHI.batch_control"
        etl_batch_date_df = pd.read_sql(etl_batch_date_query, connection)

        if etl_batch_date_df.empty:
            print("No ETL batch date found in the batch_control table.")
            return
        
        etl_batch_date = etl_batch_date_df.iloc[0, 0]

        query = f"""
        SELECT * FROM {table_name}@dblink
        WHERE UPDATE_TIMESTAMP >= :etl_batch_date
        """
        
        df = pd.read_sql(query, con=connection, params={'etl_batch_date': etl_batch_date})

        if df.empty:
            print(f"No data found for {table_name} for the specified update_timestamp.")
            return

        s3_key = f"{s3_folder}/{etl_batch_date.strftime('%Y-%m-%d')}/{table_name}.csv"
        
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        client.upload_fileobj(csv_buffer, bucket, s3_key)
        print(f"Uploaded {table_name} to s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"Error exporting {table_name}: {e}")
    finally:
        connection.close()

# Run the export function for CUSTOMERS
export_table('CUSTOMERS', 'CUSTOMERS')