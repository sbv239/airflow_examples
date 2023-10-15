from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
'hw_e-mihalev_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple DAG',
    start_date=datetime(2023, 10, 15),
    catchup=False,
    tags=['example']
) as dag:
    for i in range(10):
        task1 = BashOperator(task_id='current_directory'+str(i), bash_command=f"echo {i}")
    def func(task_number):
        print(f'task number is: {task_number}')
    for j in range(20):
        task2 = PythonOperator(task_id='func'+str(j), python_callable=func, op_kwargs={'task_number':j})
    task1 >> task2