from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_a-kramarenko_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='lesson 11 task 7 DAG',
    schedule_interval=timedelta(minutes=1)
    start_date=datetime(2023, 10, 27),
) as dag:
    
    def print_numbers(task_number, **kwargs):
        print(f"task number is: {task_number}")
        print(ts, run_id)


    for i in range(20):
        task1 = PythonOperator(
            task_id='print_number' + str(i),
            python_callable=print_numbers,
            op_kwargs={'task_number': i},
        )
        task1