"""
Lesson KC Airflow
Task 9
"""
from datetime import datetime, timedelta
#from textwrap import dedent

from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def xcon_var_push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def xcon_var_pull(ti):
    xcom_test_var = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='xcon_var_push'
    )
    print(xcom_test_var)


with DAG(
    'Task_9',
    # DAG dafault parameters
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
    tags=['i-nechushkin-20_Task_9'],
) as dag:

    t1 = PythonOperator(
        task_id='xcon_var_push',
        python_callable=xcon_var_push,
    )

    t2 = PythonOperator(
        task_id='xcon_var_pull',
        python_callable=xcon_var_pull,
    )

    t1 >> t2
