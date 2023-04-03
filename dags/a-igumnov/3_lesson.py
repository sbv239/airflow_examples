from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'a-igumnov_task_3',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'hw_3_a-igumnov',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_3_a-igumnov']


) as dag:

    def print_task_num(task_number):
        print(f'task_num is {task_number}')

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id = f'hw_2_{i}',
                bash_command = f'echo task loop {i}'
            )
        else:
            t2 = PythonOperator(
                task_id = f'hw_2_{i}',
                python_callable = print_task_num,
                op_kwargs = {'task_number' : i}
            )

    t1 >> t2