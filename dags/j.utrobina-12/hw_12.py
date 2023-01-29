from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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
    'hw_12_utrobina',
    catchup=False,
    default_args=default_args,
    schedule_interval='30 15 * * *',
    start_date=datetime(2023, 1, 21),
    description='etl',
    tags=['j-utrobina']
)

def read_v():
    print(Variable.get('is_startml'))

task1 = PythonOperator(
        task_id='read_v',
        dag=dag,
        python_callable=read_v
    )

    
task1