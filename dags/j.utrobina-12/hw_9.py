from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

dag = DAG(
    'hw_9_utrobina',
    catchup=False,
    default_args=default_args,
    schedule_interval='30 15 * * *',
    start_date=datetime(2023, 1, 21),
    description='etl',
    tags=['j-utrobina']
)

def push_xCom(ti):
    ti.xcom_push(
    key='sample_xcom_key',
    value='xcom test')
    
def get_xCom(ti):
    xCom = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids = 'push_x_com' 
    )
    print(xCom)
    

task1 = PythonOperator(
        task_id='push_x_com',
        dag=dag,
        python_callable=push_xCom
    )

task2 = PythonOperator(
        task_id='get_x_com',  
        dag=dag,
        python_callable=get_xCom
    )
    
task1 >> task2