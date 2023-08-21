from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta  # Import the necessary module

def bash_task_command(task_number):
    return f"echo Bash task {task_number}"

def python_task_function(task_number, **kwargs):
    print(f"Python task number is: {task_number}")

default_args = {
    'owner': 'kimsanova',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 21),  # Set your desired start date
    'schedule_interval': None,  # Set to None if you want manual triggering
    'catchup': False,


dag = DAG(
    'hw_b-kimsanova-22_2',
    default_args=default_args,
)

with dag:
    # Creating BashOperator tasks
    bash_tasks = []
    for i in range(1, 11):
        bash_task = BashOperator(
            task_id=f'bash_task_{i}',
            bash_command=bash_task_command(i),
        )
        bash_tasks.append(bash_task)

    # Creating PythonOperator tasks
    for i in range(1, 21):
        python_task = PythonOperator(
            task_id=f'python_task_{i}',
            python_callable=python_task_function,
            op_kwargs={'task_number': i},
        )

        # Set the dependencies for PythonOperator tasks
        for bash_task in bash_tasks:
            bash_task >> python_task

