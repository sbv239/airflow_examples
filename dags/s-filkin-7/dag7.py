from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def python_set_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value="xcom test"
    )

def python_print_task(ti):
    sample_xcom_key = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='t1'
    )
    print(sample_xcom_key)
    return 'ok'

with DAG(
    's-filkin-7-dag7',
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
        python_callable=python_set_xcom,

    )

    t2 = PythonOperator(
        task_id='t2', 
        python_callable=python_print_task,

    )

    t1 >> t2