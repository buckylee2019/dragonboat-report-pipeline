
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import sys
from airflow.providers.http.operators.http import SimpleHttpOperator
sys.path.insert(0, '/opt/airflow/scripts')  # Replace 'path/to/another_directory' with the actual directory path
import loaddata
# Define the DAG and its properties


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

clear_table = PythonOperator(
        task_id = 'drop_and_create_table',
        python_callable=loaddata.exec_sql,
        op_args=['truncate_and_create_all.sql'],
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

loadItem2DB = PythonOperator(
        task_id='load_Item2DB',
        python_callable=loaddata.upsert_item,
        op_args=['Item.csv'],
        dag=dag
    )
loadContacts2DB = PythonOperator(
        task_id='load_Contacts2DB',
        python_callable=loaddata.upsert_contacts,
        op_args=['Contacts.csv'],
        dag=dag
    )

loadMember2DB = PythonOperator(
        task_id='load_Member2DB',
        python_callable=loaddata.upsert_memberfee,
        op_args=['Memberfee.csv'],
        dag=dag
    )

loadAttendance2DB = PythonOperator(
        task_id='load_Attendance2DB',
        python_callable=loaddata.upsert_attendance,
        op_args=['Attendance.csv'],
        dag=dag
    )
# Set the task dependencies
loadExpense2DB = PythonOperator(
        task_id='load_Expense2DB',
        python_callable=loaddata.upsert_expense,
        op_args=['Expense.csv'],
        dag=dag
    )

loadPayment2DB = PythonOperator(
        task_id='load_Payment2DB',
        python_callable=loaddata.upsert_payment,
        op_args=['Payment.csv'],
        dag=dag
    )
download_detail_report = PythonOperator(
        task_id = 'download_detail_report',
        python_callable=loaddata.export_query,
        op_args=['report.sql','detail_report.csv'],
        dag=dag
)

download_attendance_report = PythonOperator(
        task_id = 'download_attendance_rp',
        python_callable=loaddata.export_query,
        op_args=['attendance.sql','attendance_report.csv'],
        dag=dag
)
download_summary_report = PythonOperator(
        task_id = 'download_summary_rp',
        python_callable=loaddata.export_query,
        op_args=['summarize_report.sql','financial_report.csv'],
        dag=dag
)
upload_CSV_attendance = BashOperator(
    task_id='upload_CSV_attendance',
    bash_command='python /opt/airflow/scripts/gsupdate.py {0} {1} {2}'.format('IBM\ Dragon\ Boat', "RP_Attendance", "attendance_report.csv"),
    dag=dag
)

upload_CSV_summary = BashOperator(
    task_id='upload_CSV_summary',
    bash_command='python /opt/airflow/scripts/gsupdate.py {0} {1} {2}'.format('IBM\ Dragon\ Boat', "Final\ Report", "financial_report.csv"),
    dag=dag
)


upload_CSV_detail = BashOperator(
    task_id='uploadcsv2detail',
    bash_command='python /opt/airflow/scripts/gsupdate.py {0} {1} {2}'.format('IBM\ Dragon\ Boat', "Expense\ Detail", "detail_report.csv"),
    dag=dag
)

notify_unpaid = BashOperator(
    task_id='notify_unpaid_member',
    bash_command='python /opt/airflow/scripts/line_notification.py',
    dag=dag
)
clear_table >> download_item
[download_item,download_memberfee,download_contacts,download_attendance,download_expense,download_payment] >> (loadContacts2DB)
# download_item.set_downstream([loadItem2DB])
# download_memberfee.set_downstream([loadMember2DB])
# download_contacts.set_downstream([loadContacts2DB])
# download_attendance.set_downstream([loadAttendance2DB])
# download_expense.set_downstream([loadExpense2DB])
# download_payment.set_downstream([loadPayment2DB])

loadContacts2DB >> [loadPayment2DB,loadItem2DB,loadMember2DB,loadMember2DB,loadAttendance2DB,loadExpense2DB]>>(download_detail_report)
download_detail_report >> upload_CSV_detail
loadAttendance2DB >> download_attendance_report >> upload_CSV_attendance
upload_CSV_detail >> download_summary_report >> upload_CSV_summary >> notify_unpaid
# loadMember2DB.set_upstream(download_report)
# loadContacts2DB.set_upstream(download_report)
# loadAttendance2DB.set_upstream(download_report)
# loadExpense2DB.set_upstream(download_report)