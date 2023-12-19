import csv
import pandas as pd
import os
import requests
import json
from airflow.models import Variable
from retrying import retry
import sys
# Define the retry decorator

import http.client

DB2_USER=Variable.get('DB2_USER')
DB2_PWD=Variable.get('DB2_PWD')
SQL_DIR = '/opt/airflow/dags/'
DATA_DIR = "/opt/airflow/scripts/data/"
def get_token():
    
    REST_API_HOSTNAME = "bpe61bfd0365e9u4psdglite.db2.cloud.ibm.com"
    conn = http.client.HTTPSConnection(REST_API_HOSTNAME)

    payload = {"userid":f'{DB2_USER}',"password":f"{DB2_PWD}"}
    DEPLOYMENT_ID = "crn:v1:bluemix:public:dashdb-for-transactions:us-south:a/4d730d558d9044f3b53d85ccd50e1a1f:570a976a-c79f-4820-92a6-48467707a055::"
    headers = {
        'content-type': "application/json",
        'x-deployment-id': f"{DEPLOYMENT_ID}"
        }

    conn.request("POST", "/dbapi/v4/auth/tokens", json.dumps(payload), headers)

    res = conn.getresponse()
    data = res.read()

    print(data.decode("utf-8"))
    url = "https://bpe61bfd0365e9u4psdglite.db2.cloud.ibm.com/dbapi/v4/auth/tokens"

    payload = json.dumps({
    "userid":  f"{DB2_USER}",
    "password":  f"{DB2_PWD}"
    })
    
    headers = {
    'Content-Type': 'application/json',
    'x-deployment-id': 'crn:v1:bluemix:public:dashdb-for-transactions:us-south:a/4d730d558d9044f3b53d85ccd50e1a1f:570a976a-c79f-4820-92a6-48467707a055::'
    }

    try:
        response = requests.request("POST", url, headers=headers, data=payload)

        # Check the status code
        if response.status_code == 200:
            # Request was successful, handle the response data
            return response.json()['token']
            # Process the data as needed
        else:
            # Request returned an error status code, handle the error
            print(f'Error: {response.status_code} - {response.text}')

    except requests.exceptions.RequestException as e:
    # Exception occurred during the request, handle the exception
        print(f'Error: {e}')

def upsert_contacts(filename):
    

    dtype_dict = {'PHONE': str, 'IBMER': str}
    df = pd.read_csv(DATA_DIR+filename, sep=",",header=0,dtype=dtype_dict)
    df.fillna('',inplace=True)

    def print_stmt(row):

        BIRTHDAY = row['BIRTHDAY'].replace('/','-')
        if row['BIRTHDAY'] == '':  
            BIRTHDAY = '0001-01-01' 
        return (f"INSERT INTO CONTACTS (MEMBER_ID,CHNAME, ENGNAME, ID, BIRTHDAY, PHONE, IBMER, COMPETITION)  \
                 VALUES ('{row['MEMBERID']}','{row['CHNAME']}','{row['ENGNAME']}','{row['ID']}','{BIRTHDAY}','{row['PHONE']}','{row['IBMER']}','{row['COMPETITION']}');"
    )
    df['sql'] = df.apply(print_stmt,axis=1)

    
    with open("/opt/airflow/dags/sql/insert_contacts.sql",'w') as f:
        f.write(''.join(list(df['sql'])))
    

# Parse the data 
def upsert_attendance(filename):
    # exec_sql("TRUNCATE TABLE ATTENDANCES IMMEDIATE;")
    def print_stmt(row):
        return (f"INSERT INTO ATTENDANCES (MEMBER_ID,PRACTICE_DATE)   VALUES ('{row['MEMBERID']}','{row['Date']}');"
    )
    df = pd.read_csv(DATA_DIR+filename, sep=",",header=0)

    #Transfrom the dataframe
    df = df.drop(["Name"],axis=1)
    df = df.melt(id_vars='MEMBERID', var_name='Date', value_name='Attendence')
    
    df.dropna(inplace=True)
    df.replace('v','Y',inplace=True)
    #Print the transformed dataframe
    df['sql'] = df.apply(print_stmt,axis=1)
    
    with open("/opt/airflow/dags/sql/insert_attendance.sql",'w') as f:
        f.write(''.join(list(df['sql'])))
def upsert_memberfee(filename):
    # exec_sql("TRUNCATE TABLE MEMBERFEE IMMEDIATE;")
    def print_stmt(row):
        return f"INSERT INTO MEMBERFEE (MEMBER_ID,PAID_DATE)   VALUES ('{row['MEMBERID']}','{row['DATE']}');"

    df = pd.read_csv(DATA_DIR+filename, sep=",",header=0)
    df.dropna(inplace=True)
    df['sql'] = df.apply(print_stmt,axis=1)
    with open("/opt/airflow/dags/sql/insert_memberfee.sql",'w') as f:
        f.write(''.join(list(df['sql'])))

def upsert_item(filename):
    # exec_sql("TRUNCATE TABLE ITEM IMMEDIATE;")
    def print_stmt(row):
        return (f"INSERT INTO ITEM (EXPENSE_NAME,AMOUNT,MEMBER)   VALUES ('{row['EXPENSE_NAME']}','{row['AMOUNT']}','{row['MEMBER']}');")

    df = pd.read_csv(DATA_DIR+filename, sep=",",header=0,index_col=False)
    df['sql'] = df.apply(print_stmt,axis=1)
    with open("/opt/airflow/dags/sql/insert_item.sql",'w') as f:
        f.write(''.join(list(df['sql'])))

def upsert_expense(filename):
    # exec_sql("TRUNCATE TABLE EXPENSES IMMEDIATE;")
    def print_stmt(row):
        return (f"INSERT INTO EXPENSES (CHNAME, MEMBER_ID, EXPENSE_NAME, QTY, UPDATE_DATE)   VALUES ('{row['NAME']}','{row['MEMBER_ID']}','{row['EXPENSE_NAME']}','{row['QTY']}','{row['UPDATE_DATE']}');")

    df = pd.read_csv(DATA_DIR+filename, sep=",",header=0)
    df['sql'] = df.apply(print_stmt,axis=1)
    with open("/opt/airflow/dags/sql/insert_expense.sql",'w') as f:
        f.write(''.join(list(df['sql'])))

    

def upsert_payment(filename):
    # exec_sql("TRUNCATE TABLE PAYMENT IMMEDIATE;")
    def print_stmt(row):
        return (f"INSERT INTO PAYMENT (MEMBER_ID, DESCRIPTION, PAID_AMOUNT, PAYMENT_DATE)   VALUES ('{row['MEMBER_ID']}','{row['DESCRIPTION']}','{row['PAID_AMOUNT']}','{row['DATE']}');")

    df = pd.read_csv(DATA_DIR+filename, sep=",",header=0)
    df['sql'] = df.apply(print_stmt,axis=1)
    with open("/opt/airflow/dags/sql/insert_payment.sql",'w') as f:
        f.write(''.join(list(df['sql'])))


def exec_sql(stmt):

    REST_API_HOSTNAME = "bpe61bfd0365e9u4psdglite.db2.cloud.ibm.com"
    conn = http.client.HTTPSConnection(REST_API_HOSTNAME)

    
    DEPLOYMENT_ID = "crn:v1:bluemix:public:dashdb-for-transactions:us-south:a/4d730d558d9044f3b53d85ccd50e1a1f:570a976a-c79f-4820-92a6-48467707a055::"

    print(stmt)
    if '.sql' in stmt:
        with open(SQL_DIR + stmt) as f:
            stmt = ' '.join(f.readlines())
    

    payload = {"commands":f"{stmt}","limit":50,"separator":";","stop_on_error":"no"}

    headers = {
        'content-type': "application/json",
        'authorization': f"Bearer {get_token()}",
        'x-deployment-id': f"{DEPLOYMENT_ID}"
        }

    conn.request("POST", "/dbapi/v4/sql_jobs", json.dumps(payload), headers)

    res = conn.getresponse()
    data = res.read()
    print(data)

def check_job(jobid):
    

    REST_API_HOSTNAME = "bpe61bfd0365e9u4psdglite.db2.cloud.ibm.com"
    conn = http.client.HTTPSConnection(REST_API_HOSTNAME)

    
    DEPLOYMENT_ID = "crn:v1:bluemix:public:dashdb-for-transactions:us-south:a/4d730d558d9044f3b53d85ccd50e1a1f:570a976a-c79f-4820-92a6-48467707a055::"

    payload = {}

    headers = {
        'content-type': "application/json",
        'authorization': f"Bearer {get_token()}",
        'x-deployment-id': f"{DEPLOYMENT_ID}"
        }

    conn.request("GET", f"/dbapi/v4/sql_jobs/{jobid}", json.dumps(payload), headers)

    res = conn.getresponse()
    response = res.read()
    print(response)

    try:
        

        # Check the status code
        if response.status_code == 200:
            data = response.json()['results']
            
            if isinstance(data, list):
                for item in data:
                    if 'error' in item:
                        # Handle the error within the response
                        error_message = item['error']
                        print (f'Error in response: {error_message}')
                        sys.exit(1)
    
                    else:

                        return (f"Row affected {item['rows_affected']}")
            else:
                # Handle unexpected response format

                return response.json()['status']
                
                # Additional error handling logic or actions
                
        else:
            # Request returned an error status code, handle the error

            print(f'Error: {response.status_code} - {response.text}')
            sys.exit(1)

    except requests.exceptions.RequestException as e:
        # Exception occurred during the request, handle the exception
        print(f'Error: {e}')
        sys.exit(1)

def export_query(sqlfile, filename):
    
    print(stmt)
    with open(SQL_DIR+sqlfile) as f:
        stmt = ' '.join(f.readlines())
    payload = json.dumps({
      "command": stmt,
    })
    
    
    REST_API_HOSTNAME = "bpe61bfd0365e9u4psdglite.db2.cloud.ibm.com"
    conn = http.client.HTTPSConnection(REST_API_HOSTNAME)

    print(filename)
    DEPLOYMENT_ID = "crn:v1:bluemix:public:dashdb-for-transactions:us-south:a/4d730d558d9044f3b53d85ccd50e1a1f:570a976a-c79f-4820-92a6-48467707a055::"


    headers = {
        'content-type': "application/json",
        'authorization': f"Bearer {get_token()}",
        'x-deployment-id': f"{DEPLOYMENT_ID}"
        }

    conn.request("POST", f"/dbapi/v4/sql_query_export/", json.dumps(payload), headers)

    res = conn.getresponse()
    response = res.read()
    print(response)
    try:
        response = requests.request("POST", url, headers=headers, data=payload)

        # Check the status code
        if response.status_code == 200 or response.status_code == 201:
            # Request was successful, handle the response data
            with open('/opt/airflow/scripts/data/'+filename,'w') as f:
                
                f.write(response.text)
            return response.text
            # Process the data as needed
        else:
            # Request returned an error status code, handle the error
            print(f'Error: {response.status_code} - {response.text}')

    except requests.exceptions.RequestException as e:
    # Exception occurred during the request, handle the exception
        print(f'Error: {e}')
    

# insert_item()

# upsert_contacts()

# upsert_attendance()