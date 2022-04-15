# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Параметры по умолчанию для тасок
default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'f-dubjago-7_dag2',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 10),
        tags=['df']
) as dag:
    for i in range(10):
        t = BashOperator(
            task_id='BashOperator' + str(i),
            bash_command=f'echo {i}',
        )


    def print_task_number(task_number, **kwargs):
        print(f'task number is: {task_number}')


    for i in range(20):
        run_this = PythonOperator(
            task_id='print_task_number' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )

    t >> run_this
