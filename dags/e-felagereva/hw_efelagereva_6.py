from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def print_i(task_number, **kwargs):
    print(f"task number is: {task_number}")

with DAG(
    'hw_efelagereva_3',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}, description = 'dag with for',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 8, 20)
) as dag:
    for i in range(10):
            t1 = BashOperator(
                task_id = f'bash_task_{i}',
                bash_command = f'echo $NUMBER',
                env = {'NUMBER' : f'{i}'}
            )
    for i in range(20):
            t2 = PythonOperator(
                task_id = f'python_task_{i}',
                python_callable = print_i,
                op_kwargs = {'task_id': f'{i}'}
            )
