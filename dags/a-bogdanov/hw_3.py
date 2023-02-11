"""
hw_2.py DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_3_a-bogdanov',  # уникальное имя DAG
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },

        description='Lesson 11, Task 3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,

        tags=['Lesson_11_Task_3'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print_in_bash_' + str(i),
            bash_command=f"echo line number {i}", # передаем путь pwd в bash
        )

    def task_number(ds, task_number, **kwargs):
        print(kwargs)
        print(ds)
        return f"task number is: {task_number}"

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task_num_' + str(i),
            python_callable=task_number, # передаем функцию print_context
            op_kwargs={'task_number': i} # передаем значение i в функцию в виде словаря
        )


    t1 >> t2
