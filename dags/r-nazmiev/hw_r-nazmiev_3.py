"""
My second DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_r-nazmiev_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='A simple practice DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023,7,17),
    catchup=False,
    tags=['example'],
) as dag:
    
    def do_majic(task_number):
        print(f"task number is: {task_number}")
        
    for i in range(30):
        
        if i < 10:
            t1 = BashOperator(
                task_id = f"t1_{i}",
                bash_command =  f"echo {i}",
            )
        else:
            t2 = PythonOperator(
                task_id = f"t2_{i}",
                python_callable = do_majic,
                op_kwargs = {'task_number': i},
            )
        
    t1>>t2
