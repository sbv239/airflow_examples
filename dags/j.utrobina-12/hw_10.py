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
    'hw_10_utrobina',
    catchup=False,
    default_args=default_args,
    schedule_interval='30 15 * * *',
    start_date=datetime(2023, 1, 21),
    description='etl',
    tags=['j-utrobina']
)

def push_str():
    return "Airflow tracks everything"
    
def get_str(ti):
    xCom = ti.xcom_pull(
        key='return_value',
        task_ids = 'push_string' 
    )
    print(xCom)
    

task1 = PythonOperator(
        task_id='push_string',
        dag=dag,
        python_callable=push_str
    )

task2 = PythonOperator(
        task_id='get_string',  
        dag=dag,
        python_callable=get_str
    )
    
task1 >> task2