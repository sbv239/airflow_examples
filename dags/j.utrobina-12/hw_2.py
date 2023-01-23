from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

dag = DAG(
    'hw_2_utrobina',
    catchup=False,
    default_args=default_args,
    schedule_interval='30 15 * * *',
    description='etl',
    tags=['j-utrobina']
)

def print_task_number(task_number):
    print(f"task number is: {task_number}")

# Генерируем таски в цикле - так тоже можно
for i in range(30):
    if i < 10:
        task = BashOperator(
            task_id = 'echo_task_' + str(i),
            bash_command=f"echo {i}",
            dag=dag
        )
    else:
        task = PythonOperator(
        task_id='print_task_number_' + str(i),  # в id можно делать все, что разрешают строки в python
        dag=dag,
        python_callable=print_task_number,
        op_kwargs={'task_number': i}
    )
    
    task