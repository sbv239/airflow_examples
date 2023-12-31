from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_date(ds):
    print(ds)
    return f'Current date: {ds}'
    
with DAG(
    'hw_1-maks-novikov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    
    description='Try a new dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 16),
    catchup=False,
    tags=['hw_1-maks-novikov'],
) as dag:
    
    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
        dag=dag
    )
    
    t2 = PythonOperator(
        task_id = 'print_date',
        python_callable=print_date
    )
    
    t1 >> t2
    
