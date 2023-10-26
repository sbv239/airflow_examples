from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_a-kramarenko_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='lesson 11 task 2 DAG',
    schedule_interval=timedelta(days=1)
    start_date=datetime(2023, 10, 26),
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='pwd',
    )

    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    task2 = PythonOperator(
        task_id='taks2',
        python_callable=print_context,
    )
    
    task1 >> task2