from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'task_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='An attempt to create DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['attempt'],
) as dag:
    
    for i in range(10):
        bash_task = BashOperator(
            task_id = 'bash_part' + str(i),
            bash_command=f"echo {i}",
        )
        
    def print_number(task_number):
        print(f"task number is: {task_number}")
    
    for i in range(11, 30):
        python_task = PythonOperator(
            task_id = 'python_part' + str(i),
            python_callable=print_number,
            op_kwargs={'task_number': i},
        )
               
    bash_task >> python_task
    
    bash_task.doc_md = dedent(
        """\
        ### Task Documentation
        bash_task is used for running a number of tasks with *BashOperator* using **for** and `str(i)`
        """
        
    python_task.doc_md = dedent(
        """\
        ### Task Documentation
        python_task is used for running a number of tasks with *PythonOperator* using **for** and `str(i)`
        """