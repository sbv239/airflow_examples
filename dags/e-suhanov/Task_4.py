"""
# Lesson Airflow. Task 4.
The program executes: 
- 10 bash commands (print 1,2,3, ..., 10) 
- 20 python prints (print tasks number)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'hw_e-suhanov_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Task 4 e-suhanov',
    start_date=datetime(2023, 10, 26),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['e-suhanov', 'task_4']
) as dag:
    def print_task_number(task_number, **kwargs):
        print(f'task number is: {task_number}')

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f'BashOperator_{i}',
                bash_command=f'echo {i}')
        else:
            t2 = PythonOperator(
                task_id=f'PythonOperator_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i})
    
    t1.doc_md = dedent(
        """
        #### Task Documentation
        There are **10** tasks in the cycle. 
        #Each command executes `bash` - `echo`

        *11 lesson ML*
        """)

    t1.doc_md = dedent(
        """
        #### Task Documentation
        There are **20** tasks in the cycle. 
        #Each command executes `python operator`. 
        #Each task print the task number.

        *11 lesson ML*
        """)
    
    t1 >> t2