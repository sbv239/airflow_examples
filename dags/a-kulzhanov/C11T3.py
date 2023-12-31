from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent


# def print_da(ds, **kwargs):
#     print("This is ds date")
#     print(ds)
#     return 'Ok'
#

def print_task(task_number):
    print(f'task number is: {task_number}')

with DAG(
    'aakulzhanov_task_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
   },
    description='A simple Task 3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    for i in range(10):
        task_Bash = BashOperator(
            task_id='print_Bash_comm' + str(i),
            depends_on_past=False,
            bash_command=f'echo {i}'
        )
        task_Bash.doc_md = dedent(
            """
            #First description `while True:` 
            ##This second *paragraph*
            ###This third cool **paragraph**
            """
        )
    for j in range(20):
        task_Python = PythonOperator(
            task_id='print_Python_comm' + str(j),
            op_kwargs={'task_number': j},
            python_callable=print_task
        )
        task_Python.doc_md = dedent(
            """
                #First description for Python `while True:` 
                ##This second too *paragraph*
                ###This third cool too **paragraph**
            """
        )

    task_Bash >> task_Python
