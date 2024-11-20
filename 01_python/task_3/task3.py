import os
import boto3 as bt
import sys
import os
import threading
import datetime
sys.path.append(os.path.abspath(".."))
from dotenv import load_dotenv, dotenv_values
from config import tables,bucket, cur_path
load_dotenv()
access_key=os.getenv('access_key')
secret_access_key=os.getenv('secret_access_key')


# Create an S3 client
client = bt.client(
    's3'
    # , aws_access_key_id=access_key,
    #       aws_secret_access_key=secret_access_key
)

# Create the S3 bucket
try:
    client.create_bucket(Bucket=bucket)
    print(f"Bucket '{bucket}' was successfully created.")
# Verify bucket existence
    response = client.head_bucket(Bucket=bucket)
    print(f"Bucket '{bucket}' exists and is accessible.")
except Exception as e:
    print(f"An error occurred while creating the bucket: {e}")


def check_file_exists(client, bucket, original_key):
    """Check if file exists in S3"""
    try:
        client.head_object(Bucket=bucket, Key=original_key)
        return True
    except:
        return False

def create_backup(client, bucket, schema, table_name, filename):
    """Create backup of existing file with timestamp"""
    original_key = f"{schema}/{table_name}/{filename}"
    
    # Check if original file exists
    if check_file_exists(client, bucket, original_key):
        # Generate timestamp for backup
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_key = f"{schema}/{table_name}/backup_{timestamp}_{filename}"
        
        try:
            # Copy existing file to backup location
            client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': original_key},
                Key=backup_key
            )
            print(f"Created backup: {backup_key}")
        except Exception as e:
            print(f"Error creating backup: {e}")


def upload_table_to_s3(schema, table_name, columns):

    filename=f"{table_name}.csv"
    s3_key=f"{schema}/{table_name}/{filename}"
    filepath= os.path.join(cur_path,schema,filename)
    create_backup(client, bucket, schema, table_name, filename)

    target_bucket = bucket
    subfolder_01 = f'{schema}/{table_name}/{filename}'
    subfolder_02 = f'{schema}/{table_name}/{filename}'
    client.put_object(Bucket=target_bucket, Key=subfolder_01)
    client.put_object(Bucket=target_bucket, Key=subfolder_02)
    

  

    # Open the file and upload it to S3
    try:
        with open(filepath, 'rb') as data:
            client.upload_fileobj(data, bucket, s3_key)
            print(f"Uploaded '{s3_key}' to bucket '{bucket}'")
    except FileNotFoundError:
        print(f"File not found: {filepath}")
    except Exception as e:
        print(f"An error occurred while uploading the file: {e}")

    #open the file
    data = open(filepath,'rb')
    client.upload_fileobj(data,target_bucket,s3_key)
   



threads = []
for schema, table_list in tables.items():
    for table_name, columns in table_list.items():
        thread = threading.Thread(target=upload_table_to_s3, args=(schema, table_name, columns))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


#load data into S3
#client.upload_file(filename,bucket,file)


# client.delete_object(Bucket='sruthi-973', Key='offices.csv')
# client.delete_object(Bucket='sruthi-973', Key='employees.csv')
# client.delete_object(Bucket='sruthi-973', Key='products.csv')
# client.delete_object(Bucket='sruthi-973', Key='productlines.csv')
# client.delete_object(Bucket='sruthi-973', Key='orders.csv')
# client.delete_object(Bucket='sruthi-973', Key='orderdetails.csv')
# client.delete_object(Bucket='sruthi-973', Key='payments.csv')
# client.delete_object(Bucket='sruthi-973', Key='customers.csv')
#client.delete_object(Bucket='sruthi-973', Key='CLASSIC_MODELS/offices/')

 #retrieve object info from bucket
all_objects = client.list_objects(Bucket=bucket)
    # iterate through Metadata and list objects
for a in all_objects['Contents']:
    print(a['Key'], a['LastModified'])