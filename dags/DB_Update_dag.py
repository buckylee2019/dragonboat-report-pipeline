
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import sys
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.suite.transfers.sql_to_sheets import SQLToGoogleSheetsOperator
sys.path.insert(0, '/opt/airflow/scripts')  # Replace 'path/to/another_directory' with the actual directory path
import loaddata
# Define the DAG and its properties
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'dragon_boat_accounting',
    description='A simple DAG to run a Python file',
    # schedule_interval='0 0 * * *',  # Daily at midnight
    schedule_interval='0 2 * * *',  # Cron expression for every 5 minutes
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Define the task that runs the Python file

clear_table = PostgresOperator(
        task_id="clear_table",
        sql="sql/truncate_and_create_all.sql",
        postgres_conn_id="postgres_default",
        dag=dag
    )

download_attendance = BashOperator(
    task_id='download_attendance',
    bash_command='python /opt/airflow/scripts/gsdownload.py {0} {1} {2}'.format('IBM\ Dragon\ Boat', "Attendance", "Attendance.csv"),
    dag=dag
)


download_contacts = BashOperator(
    task_id='download_contacts',
    bash_command='python /opt/airflow/scripts/gsdownload.py {0} {1} {2}'.format('IBM\ Dragon\ Boat', "社員清單", "Contacts.csv"),
    dag=dag
)


download_memberfee = BashOperator(
    task_id='download_Memberfee',
    bash_command='python /opt/airflow/scripts/gsdownload.py {0} {1} {2}'.format('IBM\ Dragon\ Boat', "Memberfee", "Memberfee.csv"),
    dag=dag
)

download_item = BashOperator(
    task_id='download_item',
    bash_command='python /opt/airflow/scripts/gsdownload.py {0} {1} {2}'.format('IBM\ Dragon\ Boat', "Item", "Item.csv"),
    dag=dag
)

download_expense = BashOperator(
    task_id='download_expense',
    bash_command='python /opt/airflow/scripts/gsdownload.py {0} {1} {2}'.format('IBM\ Dragon\ Boat', "Expense", "Expense.csv"),
    dag=dag
)

download_payment = BashOperator(
    task_id='download_payment',
    bash_command='python /opt/airflow/scripts/gsdownload.py {0} {1} {2}'.format('IBM\ Dragon\ Boat', "PaidRecord", "Payment.csv"),
    dag=dag
)

download_announce = BashOperator(
    task_id='download_announce',
    bash_command='python /opt/airflow/scripts/gsdownload.py {0} {1} {2}'.format('IBM\ Dragon\ Boat', "Announcement", "announcement.csv"),
    dag=dag
)

create_insert_item_sql = PythonOperator(
        task_id='create_insert_item_sql',
        python_callable=loaddata.upsert_item,
        op_args=['Item.csv'],
        dag=dag
    )
insert_item = PostgresOperator(
        task_id="insert_item",
        sql="sql/insert_item.sql",
        postgres_conn_id="postgres_default",
        dag=dag
    )
create_insert_contacts_sql = PythonOperator(
        task_id='create_insert_contacts_sql',
        python_callable=loaddata.upsert_contacts,
        op_args=['Contacts.csv'],
        dag=dag
    )
insert_contacts = PostgresOperator(
        task_id="insert_contacts",
        sql="sql/insert_contacts.sql",
        postgres_conn_id="postgres_default",
        dag=dag
    )
create_insert_member_sql = PythonOperator(
        task_id='create_insert_member_sql',
        python_callable=loaddata.upsert_memberfee,
        op_args=['Memberfee.csv'],
        dag=dag
    )
insert_member = PostgresOperator(
        task_id="insert_member",
        sql="sql/insert_memberfee.sql",
        postgres_conn_id="postgres_default",
        dag=dag
    )
create_insert_attendance_sql = PythonOperator(
        task_id='create_insert_attendance_sql',
        python_callable=loaddata.upsert_attendance,
        op_args=['Attendance.csv'],
        dag=dag
    )
insert_attendance = PostgresOperator(
        task_id="insert_attendance",
        sql="sql/insert_attendance.sql",
        postgres_conn_id="postgres_default",
        dag=dag
    )
# Set the task dependencies
create_insert_expense_sql = PythonOperator(
        task_id='create_insert_expense_sql',
        python_callable=loaddata.upsert_expense,
        op_args=['Expense.csv'],
        dag=dag
    )
insert_expense = PostgresOperator(
        task_id="insert_expense",
        sql="sql/insert_expense.sql",
        postgres_conn_id="postgres_default",
        dag=dag
    )
create_insert_payment_sql = PythonOperator(
        task_id='create_insert_payment_sql',
        python_callable=loaddata.upsert_payment,
        op_args=['Payment.csv'],
        dag=dag
    )
insert_payment = PostgresOperator(
        task_id="insert_payment",
        sql="sql/insert_payment.sql",
        postgres_conn_id="postgres_default",
        dag=dag
    )
upload_sql_to_sheet = SQLToGoogleSheetsOperator(
    task_id="upload_sql_to_sheet",
    sql='sql/report.sql',
    sql_conn_id="postgres_default",
    database="airflow",
    spreadsheet_id="1oCHouAQvLN-aM9PzswXeMTklxL3CCcoEQ30AG0Wbq2c",
    spreadsheet_range="Test",
    gcp_conn_id="gcp_default",
)


upload_attendance = SQLToGoogleSheetsOperator(
    task_id="upload_attendance",
    sql='sql/attendance.sql',
    sql_conn_id="postgres_default",
    database="airflow",
    spreadsheet_id="1oCHouAQvLN-aM9PzswXeMTklxL3CCcoEQ30AG0Wbq2c",
    spreadsheet_range="RP_Attendance",
    gcp_conn_id="gcp_default",
)

upload_summary = SQLToGoogleSheetsOperator(
    task_id="upload_summary",
    sql='sql/summarize_report.sql',
    sql_conn_id="postgres_default",
    database="airflow",
    spreadsheet_id="1oCHouAQvLN-aM9PzswXeMTklxL3CCcoEQ30AG0Wbq2c",
    spreadsheet_range="Final Report",
    gcp_conn_id="gcp_default",
)


upload_detail = SQLToGoogleSheetsOperator(
    task_id="upload_detail",
    sql='sql/report.sql',
    sql_conn_id="postgres_default",
    database="airflow",
    spreadsheet_id="1oCHouAQvLN-aM9PzswXeMTklxL3CCcoEQ30AG0Wbq2c",
    spreadsheet_range="Expense Detail",
    gcp_conn_id="gcp_default",
)
notify_unpaid = BashOperator(
    task_id='notify_unpaid_member',
    bash_command='python /opt/airflow/scripts/line_notification.py',
    dag=dag
)
clear_table >> download_item
[download_item,download_memberfee,download_contacts,download_attendance,download_expense,download_payment] >> (create_insert_contacts_sql)


create_insert_contacts_sql >> [create_insert_payment_sql,create_insert_item_sql,create_insert_member_sql,create_insert_member_sql,create_insert_attendance_sql,create_insert_expense_sql]>>insert_contacts
insert_contacts >> [insert_item,insert_payment,insert_attendance,insert_member,insert_expense]

insert_attendance >> upload_attendance
upload_attendance >> upload_detail >> upload_summary >> notify_unpaid
