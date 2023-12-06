from datetime import datetime, timedelta

from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

def push_ti():
    return "Airflow tracks everything"

def pull_ti(ti):
    res = ti.xcom_pull(
        key="return_value",
        task_ids="push_xcom_ti"
    )
    print(res)

with DAG(
    'm-mihail-24_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example_10'],
) as dag:
    python_op1 = PythonOperator(
        task_id='push_xcom_ti',
        python_callable=push_ti
    )

    python_op2 = PythonOperator(
        task_id='pull_xcom_ti',
        python_callable=pull_ti
    )
    python_op1 >> python_op2
