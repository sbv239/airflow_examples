"""
Test documentation
"""
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.bash import BashOperator

def print_context(task_number):
    print(f'task number is: {task_number}')


with DAG(
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)},

    start_date= datetime(2023, 9, 18),
    dag_id="hw_6_a-ratushnyj",
    schedule_interval=timedelta(days=1),
    tags=['hw-6'],
    # Описание DAG (не тасок, а самого DAG)

) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id="task_bash_" + str(i),
            bash_command=f"echo $NUMBER",  # обратите внимание на пробел в конце!
            dag=dag,  # говорим, что таска принадлежит дагу из переменной dag
            env={"NUMBER": i},
        )



    t1
