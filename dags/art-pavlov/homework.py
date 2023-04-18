from datetime import datetime, timedelta
from textwrap import dedent

from airflow import  DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def test_push(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )

def test_pull(ti):
    ti.xcom_pull(
        task_ids = "xcom_push",
        key = "sample_xcom_key"
    )

with DAG(
    "XCOM_test",

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    description= "first try of starting DAG's",
    schedule_interval= timedelta(days=1),
    start_date= datetime(2023, 4, 17),
    catchup= False,
    tags=['idk7']
) as dag:

    t1 = PythonOperator(
        task_id="xcom_push",
        python_callable=test_push
    )

    t2 = PythonOperator(
        task_id = "xcom_pull",
        python_callable = test_pull
    )

    t1 >> t2