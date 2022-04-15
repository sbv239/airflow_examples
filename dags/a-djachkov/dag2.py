from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_2_a-djachkov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="Lesson 11 home work 1",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['a-djachkov'],
) as dag:

    for i in range(10):
        task1 = BashOperator(
            task_id='print: ' + str(i),
            bash_command=f"echo {i}"
        )


    def print_task_number(task_number):
        print(f'task number is -  {task_number}')


    for i in range(20):
        task2 = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )

    task1 >> task2
