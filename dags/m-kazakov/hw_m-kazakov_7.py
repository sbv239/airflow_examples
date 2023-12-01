from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

with DAG(
    'hw_m-kazakov_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime  
    }, 
    description = 'HW 7 m-kazakov',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['NE TROGAT and NE SMOTRET)'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='loop' + str(i),
            bash_command=f"echo {i}"
        )
        t1
    
    def print_num(task_number, ts, run_id):
        print(ts)
        print(run_id)
        print(f"task number is: {task_number}")
    for i in range(20):
        t2=PythonOperator(
            task_id='print' + str(i),
            python_callable=print_num,
            op_kwargs={'task_number': i}
        )
        t2