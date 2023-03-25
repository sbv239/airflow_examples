from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_task_number(task_number, ts, run_id):
    print(f'task number is: {task_number}')
    print(ts)
    print(run_id)
    return 'task number has printed'


with DAG(
        'hw_7_s-plaksin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Print date and working directory',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 22),
        catchup=False,
        tags=['hw_7'],
) as dag:
    for i in range(1, 11):
        task = BashOperator(
            task_id=f'bash_task_{i}',
            bash_command=f'echo {i} '
        )

    for task_print in range(11, 31):
        task2 = PythonOperator(
            task_id=f'task_number_is_{task_print}',
            python_callable=print_task_number,
            op_kwargs={'task_number': int(task_print)}
        )
    task >> task2