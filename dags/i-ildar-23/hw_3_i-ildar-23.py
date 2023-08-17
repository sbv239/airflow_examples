from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_task(task_number):
    print(f'task number is: {task_number}')
    ...


with DAG(
        'hw_i-ildar-23_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 8, 16)
) as dag:
    count_task = 0
    while count_task < 30:
        if count_task < 10:
            task_bash = BashOperator(
                task_id='hw_3_bash_' + str(count_task),
                bash_command=f'echo {count_task}',
            )
        else:
            task_python = PythonOperator(
                task_id='hw_3_python' + str(count_task),
                python_callable=print_task,
                op_kwargs={'task_number': int(count_task)},
            )

        count_task += 1
        task_bash >> task_python

