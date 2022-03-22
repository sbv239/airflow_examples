from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
        'dag_3_m-tsaj',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Cyclic tasks dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 3, 20),
        catchup=False,
) as dag:
    for i in range(10):
        bash_task = BashOperator(
            task_id=f'echo_task_{i}',
            bash_command=f'echo {i}',
        )


    def print_task_number(task_n: int):
        print(f'task number is: {task_n}')


    for task_number in range(20):
        python_task = PythonOperator(
            task_id=f'print_task_{task_number}',
            python_callable=print_task_number,
            op_kwargs={'task_n': task_number},
        )
