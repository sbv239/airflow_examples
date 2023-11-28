from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator, PythonOperator

with DAG(
    'hw_{nikacloudnikitina@gmail.com}_2', 
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        }
) as dag:
    
    t1 = BashOperator(
        task_id= 'first_try', 
        bash_command='pwd'
    )

def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'
