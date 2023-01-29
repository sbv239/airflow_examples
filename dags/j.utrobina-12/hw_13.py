from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

dag = DAG(
    'hw_13_utrobina',
    catchup=False,
    default_args=default_args,
    schedule_interval='30 15 * * *',
    start_date=datetime(2023, 1, 21),
    description='etl',
    tags=['j-utrobina']
)

def branching():
    if Variable.get('is_startml') is "True":
        return 'startml_desc'
    else:
        return 'not_startml_desc'
    
def StartML():
    print("StartML is a starter course for ambitious people")
    
def notStartML():
    print("Not a startML course, sorry")
    

task1 = BranchPythonOperator(
        task_id='branching_task',
        dag=dag,
        python_callable=branching
    )

task2 = PythonOperator(
        task_id='startml_desc',  
        dag=dag,
        python_callable=StartML
    )

task3 = PythonOperator(
        task_id='not_startml_desc',  
        dag=dag,
        python_callable=notStartML
    )
    
task1 >> [task2, task3]