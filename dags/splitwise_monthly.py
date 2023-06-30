from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime

default_args = {
    'owner': 'Bucky Lee',
    'email': ['little880536@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

export_date = datetime.now().date()
Variable.set('date_export',export_date)

with DAG('email_dag', default_args=default_args, start_date=datetime(2023, 5, 1), schedule_interval='@monthly') as dag:
    exportsplitwise = BashOperator(
    task_id='export_splitwise_record',
    #/opt/airflow/scripts/splitwise_export.py
    bash_command='python /opt/airflow/scripts/splitwise_export.py')
    send_email_task = EmailOperator(
        task_id='send_email',
        to=['little880536@gmail.com'],
        subject=f"Monthly splitwise report - {export_date}",
        html_content='<p>Splitwise 月結算, 下載並匯入MOZE 3.0 APP</p>',
        files=[f"/opt/airflow/scripts/data/splitwise_{export_date}.csv"],
    )
    exportsplitwise>>send_email_task