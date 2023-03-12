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
        t_1 = BashOperator(
            task_id = 'bash_part' + str(i),
            bash_command=f"echo {i}",
        )
        
    for i in range(11, 30):
        t_2 = PythonOperator(
            task_id = 'python_part' + str(i),
            python_callable=print_number,
            op_kwargs={'task_number': i},
        )
            
    def print_number(task_number):
            print(f"task number is: {task_number}")
            
    t_1 >> t_2
    
    t_1.doc_md = dedent(
        """\
        ### Task Documentation
        t1 is used for running a number of tasks with *BashOperator* using **for** and `str(i)`
        """
        
    t_2.doc_md = dedent(
        """\
        ### Task Documentation
        t2 is used for running a number of tasks with *PythonOperator* using **for** and `str(i)`
        """