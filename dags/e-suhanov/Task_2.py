from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

with DAG(
    'hw_e-suhanov_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_retry': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Task 2 e-suhanov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 26),
    catchup=False,
    tags=['e-suhanov', 'task_2']
) as dag:
    t1 = BashOperator(task_id='bash_pwd', bash_command='pwd')
    t2 = PythonOperator(task_id='print_context', python_callable=print_context)

    t1 >> t2