import logging
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime


@task
def variable_set():
    Variable.set(key="updated_giftcard_id", value="0", serialize_json=True)
    Variable.set(
        key="last_ids", value={"updated_giftcard_id": "0", "updated_order_id": "1"}, serialize_json=True
    )

@task
def variable_get():
    logging.info(Variable.get(key="updated_giftcard_id", deserialize_json=True))
    logging.info(Variable.get(key="last_ids", deserialize_json=True))

with DAG(
    "example",
    start_date=datetime(2021, 8, 13),
    schedule_interval=None,
    catchup=False,
) as dag:
    variable_set() >> variable_get()