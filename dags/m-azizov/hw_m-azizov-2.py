from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_m-azizov_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 25),
    catchup=False
) as dag:

    def print_task(task_number):
        print(f'task number is: {task_number}')

    for i in range(10):
        t2 = BashOperator(
            task_id='echo_task_m' + str(i),
            bash_command=f"echo {i}"
        )
    
    for i in range(10, 30):
        t1 = PythonOperator(
            task_id='print_task' + str(i),
            python_callable=print_task,
            op_kwargs={'task_number': i},
        )

    t2 >> t1