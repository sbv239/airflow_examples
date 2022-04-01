"""
 Airflow trials

"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def return_str():
    """
    returning a string
    """
    return "Airflow tracks everything"

def extract_testing_increases(ti):
    """
    Get and print key-value from Xcom
    """
    testing_increases = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids="print_string_sf"
    )
    print("checking xcom that... " + testing_increases)


with DAG(
's-filonov-6_hw9',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
},

description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 28),
    catchup=False,
    tags=['learning'],
) as dag:

    t1 = PythonOperator(
            task_id='print_string_sf',
            python_callable= return_str,
          )

    t2 = PythonOperator(
            task_id='py_xcom_extract',
            python_callable= extract_testing_increases,
          )

    t1 >> t2