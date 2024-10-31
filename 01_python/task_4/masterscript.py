import concurrent.futures
import subprocess

scripts = [
    'customers.py',
    'employees.py',
    'offices.py',
    'orderdetails.py',
    'orders.py',
    'payments.py',
    'productlines.py',
    'products.py'
]

def run_script(script):
    subprocess.run(['python', script])

with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(run_script, scripts)