import oracledb
import csv
import sys
import os
import threading
sys.path.append(os.path.abspath(".."))
from config import tables,username,password,dsn,cur_path,client_path

# Initialize Oracle client to use thick mode (adjust path to your Instant Client location)
oracledb.init_oracle_client(lib_dir=client_path ) # Adjust this path to where you installed Oracle Instant Client


# Connect to the Oracle database
connection = oracledb.connect(user=username, password=password, dsn=dsn)

# SQL query to select all data from the 'offices' table in the 'classicmodels' schema
def export_table_data(schema, table_name, columns):
    # Create a cursor to execute SQL queries
    cursor = connection.cursor()

    try:
        # Build SQL query to select specified columns from the table
        query = f"SELECT {', '.join(columns)} FROM {schema}.{table_name}"

        # Execute the query
        cursor.execute(query)

        # Fetch all rows from the executed query
        rows = cursor.fetchall()

        # Get the column names
        column_names = [col[0] for col in cursor.description]

        # Specify the output CSV file path
        csv_file = os.path.join(cur_path, f"{schema}/{table_name}.csv")

        # Write the data to a CSV file
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            # Write the column headers
            writer.writerow(column_names)
            # Write the rows of data
            writer.writerows(rows)

        print(f"Data from {schema}.{table_name} table exported to {csv_file}")
    except Exception as e:
        print(f"Error processing {schema}.{table_name}: {e}")
    finally:
        # Close the cursor after processing
        cursor.close()

# List to keep track of threads
threads = []

# Loop over schemas and tables to create threads
for schema, table_list in tables.items():
    for table_name, columns in table_list.items():
        # Create a thread for each table
        thread = threading.Thread(target=export_table_data, args=(schema, table_name, columns))
        threads.append(thread)
        thread.start()  # Start the thread

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Close the database connection
connection.close()
print("All data exported.")