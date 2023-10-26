from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_7_a-hlopov-25', 
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Task 7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 25),
        catchup=False,
        tags=['Task_7'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='echo_bash' + str(i),
            bash_command=f"echo line number {i}",
        )
    def print_context(ts, run_id, task_number):
        print(ts, run_id)
        return f"task number: {task_number}"
    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task_num_' + str(i),
            python_callable=print_context,  
            op_kwargs={'task_number': i} 
        )
    