import subprocess

def run_script(script_name):
    """
    Runs a given Python script and captures its output.
    """
    try:
        print(f"Starting script: {script_name}...")
        result = subprocess.run(["python", script_name], capture_output=True, text=True)

        if result.returncode == 0:
            print(f"Script {script_name} completed successfully.")
            print(f"Output:\n{result.stdout}")
        else:
            print(f"Script {script_name} failed.")
            print(f"Error:\n{result.stderr}")
            # Exit if any script fails
            exit(1)
    except Exception as e:
        print(f"An error occurred while running {script_name}: {e}")
        exit(1)

if __name__ == "__main__":
    print("Starting the Master Script...\n")

    # List of scripts to execute in sequence
    scripts = [
        
        "oracle_s3.py",        # Step 1: Fetch data from Oracle and upload to S3
        "truncate_script.py",  # Step 2: Truncate existing data
        "s3_stg.py",           # Step 3: Load data from S3 to the staging schema
        "stg_dw.py"            # Step 4: Transform data from staging to the data warehouse
    ]

    # Execute each script in the defined order
    for script in scripts:
        run_script(script)

    print("\nAll scripts executed successfully!")
