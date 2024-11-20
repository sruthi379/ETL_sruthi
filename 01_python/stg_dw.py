import subprocess

try:
    subprocess.run(["python", "STG_DW/offices.py"], check=True)
    
    subprocess.run(["python", "STG_DW/employees.py"], check=True)

    subprocess.run(["python", "STG_DW/customers.py"], check=True)

    subprocess.run(["python", "STG_DW/customer_history.py"], check=True)
    
    subprocess.run(["python", "STG_DW/payments.py"], check=True)
    
    subprocess.run(["python", "STG_DW/productlines.py"], check=True)

    subprocess.run(["python", "STG_DW/products.py"], check=True)

    subprocess.run(["python", "STG_DW/product_history.py"], check=True)
    
    subprocess.run(["python", "STG_DW/orders.py"], check=True)

    subprocess.run(["python", "STG_DW/orderdetails.py"], check=True)

    subprocess.run(["python", "STG_DW/dcs.py"], check=True)

    subprocess.run(["python", "STG_DW/mcs.py"], check=True)

    subprocess.run(["python", "STG_DW/dps.py"], check=True)

    subprocess.run(["python", "STG_DW/mps.py"], check=True)

except subprocess.CalledProcessError as e:
    print(f"Error occurred while running script: {e}")