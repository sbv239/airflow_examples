from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_m-kazakov_2',
    default_argc={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime  
    }, 
    description = 'EX1',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2023, 11, 29),
    catchup=False,
    tags=['NE TROGAT and NE SMOTRET)'],
) as dag:
    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd'
    )
    def print_date(ds):
        print(ds)
        return 'OK'
    
    t2=PythonOperator(
        task_id='print_date',
        python_callable=print_date
    )

    t1>>t2