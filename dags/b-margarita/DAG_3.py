from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_b-margarita_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Task 3 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['b-margarita'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'print_task_{i}',
            bash_command=f'echo_{i}',
        )

    def print_task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(10, 30):
        t2 = PythonOperator(
            task_id=f'print_task_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )

    t1 >> t2



