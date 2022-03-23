from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
with DAG\
    (
    "task_4_v_zabolotskij",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description = "DAG for task #4",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 20),
    catchup = False,
    tags = ["task_4"]

    ) as dag:


    def print_task_number(task_number):
        return f"task number is: {task_number}"

    for task in range(30):

        if task <= 10:
            bash_task = BashOperator(
                task_id = "BO_task_" + str(task),
                bash_command = f"echo {task}"            
            )
        else:
            py_task = PythonOperator(
                task_id = "PY_task_" + str(task),
                python_callable = print_task_number,
                op_kwargs = {"task_number": task}
            )

bash_task >> py_task
