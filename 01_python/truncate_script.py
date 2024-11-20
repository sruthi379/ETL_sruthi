import os
import redshift_connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Redshift configuration
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DBNAME")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

# List of tables to truncate
TABLES = [
    "offices",
    "employees",
    "customers",
    "payments",
    "orders",
    "orderdetails",
    "products",
    "productlines"
]
SCHEMA_NAME = "devstage"  # Schema name

def get_redshift_connection():
    """
    Establishes and returns a connection to Redshift.
    """
    return redshift_connector.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

def truncate_specific_tables():
    """
    Truncates specific tables in the specified schema.
    """
    connection = get_redshift_connection()
    cursor = connection.cursor()
    try:
        # Iterate through the table list and truncate each table
        for table in TABLES:
            full_table_name = f"{SCHEMA_NAME}.{table}"
            print(f"Truncating table: {full_table_name}")
            cursor.execute(f"TRUNCATE TABLE {full_table_name}")
        
        connection.commit()
        print("All specified tables have been truncated successfully.")
    except redshift_connector.Error as error:
        print(f"Error while truncating tables: {error}")
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    truncate_specific_tables()
