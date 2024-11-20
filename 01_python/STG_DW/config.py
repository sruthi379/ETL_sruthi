username = "h24sruthi"
password = "SRUTHI_SQL"
dsn = "3.86.8.206:1521/xe"

# Specify the bucket name you wish to create
bucket = "sruthi-973"
redshift_username="admin"
redshift_password= "Sruthi123"

redshift_dsn = {
    'dbname': 'dev',
    'host': 'default-workgroup.235494796624.us-east-1.redshift-serverless.amazonaws.com',
    'port': '5439'
}
cur_path=r'C:\Users\sruthi.kairam\python_tasks\task1_task2'
client_path=r"C:\Users\sruthi.kairam\Downloads\instantclient-basic-windows.x64-23.5.0.24.07\instantclient_23_5"
tables = {
    'CLASSICMODELS': {
        'CALENDAR': [
            'CALENDAR_DATE', 'DAY_OF_THE_MONTH', 'MONTH_OF_THE_YEAR', 'YEAR', 
            'MONTH_START_DATE', 'MONTH_END_DATE', 'NEXT_MONTH_START_DATE', 
            'NEXT_MONTH_END_DATE', 'PREV_MONTH_START_DATE', 'PREV_MONTH_END_DATE', 
            'YEAR_START_DATE', 'YEAR_END_DATE', 'YEAR_MONTH', 'MONTH_NAME', 
            'WEEK_OF_THE_YEAR', 'WEEK_OF_THE_MONTH', 'YEAR_WEEK', 'DAY_OF_THE_WEEK', 
            'QUARTER_START_DATE', 'QUARTER_END_DATE', 'YEAR_QUARTER'],
        'CUSTOMERS': [
            'CUSTOMERNUMBER', 'CUSTOMERNAME', 'CONTACTLASTNAME', 'CONTACTFIRSTNAME',
            'PHONE', 'ADDRESSLINE1', 'ADDRESSLINE2', 'CITY', 'STATE', 'POSTALCODE', 
            'COUNTRY', 'SALESREPEMPLOYEENUMBER', 'CREDITLIMIT'
        ],
        'EMPLOYEES': [
            'EMPLOYEENUMBER', 'LASTNAME', 'FIRSTNAME', 'EXTENSION', 'EMAIL', 
            'OFFICECODE', 'REPORTSTO', 'JOBTITLE'
        ],
        'OFFICES': [
            'OFFICECODE', 'CITY', 'PHONE', 'ADDRESSLINE1', 'ADDRESSLINE2', 'STATE', 
            'COUNTRY', 'POSTALCODE', 'TERRITORY'
        ],
        'ORDERDETAILS': [
            'ORDERNUMBER', 'PRODUCTCODE', 'QUANTITYORDERED', 'PRICEEACH', 'ORDERLINENUMBER'
        ],
        'ORDERS': [
            'ORDERNUMBER', 'ORDERDATE', 'REQUIREDDATE', 'SHIPPEDDATE', 'STATUS', 
            'COMMENTS', 'CUSTOMERNUMBER'
        ],
        'PAYMENTS': [
            'CUSTOMERNUMBER', 'CHECKNUMBER', 'PAYMENTDATE', 'AMOUNT'
        ],
        'PRODUCTLINES': [
            'PRODUCTLINE', 'TEXTDESCRIPTION', 'HTMLDESCRIPTION', 'IMAGE'
        ],
        'PRODUCTS': [
            'PRODUCTCODE', 'PRODUCTNAME', 'PRODUCTLINE', 'PRODUCTSCALE', 'PRODUCTVENDOR', 
            'PRODUCTDESCRIPTION', 'QUANTITYINSTOCK', 'BUYPRICE', 'MSRP'
        ]
    },
    'CM_20050609': {
        'CALENDAR': [
            'CALENDAR_DATE', 'DAY_OF_THE_MONTH', 'MONTH_OF_THE_YEAR', 'YEAR', 
            'MONTH_START_DATE', 'MONTH_END_DATE', 'NEXT_MONTH_START_DATE', 
            'NEXT_MONTH_END_DATE', 'PREV_MONTH_START_DATE', 'PREV_MONTH_END_DATE', 
            'YEAR_START_DATE', 'YEAR_END_DATE', 'YEAR_MONTH', 'MONTH_NAME', 
            'WEEK_OF_THE_YEAR', 'WEEK_OF_THE_MONTH', 'YEAR_WEEK', 'DAY_OF_THE_WEEK', 
            'QUARTER_START_DATE', 'QUARTER_END_DATE', 'YEAR_QUARTER'
        ],
        'CUSTOMERS': [
            'CUSTOMERNUMBER', 'CUSTOMERNAME', 'CONTACTLASTNAME', 'CONTACTFIRSTNAME', 
            'PHONE', 'ADDRESSLINE1', 'ADDRESSLINE2', 'CITY', 'STATE', 'POSTALCODE', 
            'COUNTRY', 'SALESREPEMPLOYEENUMBER', 'CREDITLIMIT', 'CREATE_TIMESTAMP', 
            'UPDATE_TIMESTAMP'
        ],
        'EMPLOYEES': [
            'EMPLOYEENUMBER', 'LASTNAME', 'FIRSTNAME', 'EXTENSION', 'EMAIL', 
            'OFFICECODE', 'REPORTSTO', 'JOBTITLE', 'CREATE_TIMESTAMP', 'UPDATE_TIMESTAMP'
        ],
        'OFFICES': [
            'OFFICECODE', 'CITY', 'PHONE', 'ADDRESSLINE1', 'ADDRESSLINE2', 'STATE', 
            'COUNTRY', 'POSTALCODE', 'TERRITORY', 'CREATE_TIMESTAMP', 'UPDATE_TIMESTAMP'
        ],
        'ORDERDETAILS': [
            'ORDERNUMBER', 'PRODUCTCODE', 'QUANTITYORDERED', 'PRICEEACH', 'ORDERLINENUMBER', 
            'CREATE_TIMESTAMP', 'UPDATE_TIMESTAMP'
        ],
        'ORDERS': [
            'ORDERNUMBER', 'ORDERDATE', 'REQUIREDDATE', 'SHIPPEDDATE', 'STATUS', 'COMMENTS', 
            'CUSTOMERNUMBER', 'CREATE_TIMESTAMP', 'UPDATE_TIMESTAMP', 'CANCELLEDDATE'
        ],
        'PAYMENTS': [
            'CUSTOMERNUMBER', 'CHECKNUMBER', 'PAYMENTDATE', 'AMOUNT', 
            'CREATE_TIMESTAMP', 'UPDATE_TIMESTAMP'
        ],
        'PRODUCTLINES': [
            'PRODUCTLINE', 'TEXTDESCRIPTION', 'HTMLDESCRIPTION', 'IMAGE', 
            'CREATE_TIMESTAMP', 'UPDATE_TIMESTAMP'
        ],
        'PRODUCTS': [
            'PRODUCTCODE', 'PRODUCTNAME', 'PRODUCTLINE', 'PRODUCTSCALE', 'PRODUCTVENDOR', 
            'PRODUCTDESCRIPTION', 'QUANTITYINSTOCK', 'BUYPRICE', 'MSRP', 
            'CREATE_TIMESTAMP', 'UPDATE_TIMESTAMP'
        ]
    }
}

