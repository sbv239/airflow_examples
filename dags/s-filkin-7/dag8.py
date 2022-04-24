from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def python_func1():
    return "Airflow tracks everything"

def python_print_task(ti):
    xcom_value = ti.xcom_pull(
        key='return_value',
        task_ids='t1'
    )
    print(xcom_value)
    return 'ok'

with DAG(
    's-filkin-7-dag8',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 16),
) as dag:

    t1 = PythonOperator(
        task_id='t1', 
        python_callable=python_func1,

    )

    t2 = PythonOperator(
        task_id='t2', 
        python_callable=python_print_task,

    )

    t1 >> t2