"""
hw_8_m_zharehina_5
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_8_m_zharehina_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='hw_8_m_zharehina_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 26),
    catchup=False,
    tags=['hw_8_m_zharehina_5'],
    ) as dag:
    
    def func1(ti):
        ti.xcom_push(key='sample_xcom_key', 
                     value='xcom test')

    def func2(ti):
        t = ti.xcom_pull(key='sample_xcom_key', 
                         task_ids='push_data')
        print(t)
    
    task1 = PythonOperator(task_id='hw_8_m_zharehina_5_func1', 
                        python_callable=func1)
    task2 = PythonOperator(task_id='hw_8_m_zharehina_5_func2', 
                        python_callable=func2)
    task1 >> task2