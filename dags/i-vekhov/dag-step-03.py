from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta

with DAG(
    'hw_3_i-vekhov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw_3_i-vekhov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 3, 29),
    catchup=False,
    tags=['hw_3_i-vekhov'],
) as dag:
    for i in range(10):
        task_bash = BashOperator(
                task_id=f'task_bash_{i}',
                bash_command=f'echo {i}'
        )

    def print_task_number(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        task_python = PythonOperator(
                task_id=f'task_python_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i})
        task_bash >> task_python
