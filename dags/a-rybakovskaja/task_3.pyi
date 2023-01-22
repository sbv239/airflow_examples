from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'print_tasks_t3',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2023, 1, 21),
    tags=['a-rybakovskaya'],

) as dag:
    def print_task(task_number):
        return print(f"task number is: {task_number}")

    for i in range(30):
        if i <= 9:
            task = BashOperator(
                task_id='echo_' + str(i),
                bash_command=f"echo {i}")
        else:
            task = PythonOperator(
                task_id='print_' + str(i),
                python_callable=print_task,
                op_kwargs={'task_number': i},
            )
        task