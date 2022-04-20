from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'dag_2_d-luzina-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  
    },
    description='Second dag lesson 11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
    
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id = 'bash_' + str(i),
            bash_command = f"echo {i}",
        )
    
    def print_num(task_number):
        print(f"task number is: {task_number}")

    for i in range(10,30):
        t2 = PythonOperator(
            task_id = 'python_' + str(i),
            python_callable = print_num,
            op_kwargs = {'task_number': i},
        )

    t1 >> t2