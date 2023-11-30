"""
HW 9
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_xcom_push(ti):
       ti.xcom_push(
               key='sample_xcom_key',
               value="xcom test"
               )
       
def get_xcom_pull(ti):
       print( ti.xcom_pull( key='sample_xcom_key', task_ids='xcom_push'))

default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_9_s-novikova-26',
    default_args=default_args,
    description='HW 9',
    start_date=datetime(2023, 11, 30),
    catchup=False,
    tags=['HW 9']
) as dag:
        t1 = PythonOperator(
            task_id='xcom_push',
            python_callable=get_xcom_push
        )
        t2 = PythonOperator(
                task_id='xcom_pull',
                python_callable=get_xcom_pull

        )

        t1 >> t2