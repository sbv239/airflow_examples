from airflow import DAG

from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_d-trubitsin_3',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 23),
    catchup=False,
    tags=['d-trubitsin_3'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f'task {i}',
            bash_command=f'echo {i}',
            dag=dag
        )

    def print_func(task_number):
        print(f"task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            op_kwargs={'task_number': i}
        )
