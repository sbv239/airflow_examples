from airflow import DAG
from textwrap import dedent
from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_y-kretov_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 27),
    catchup=False,
    tags=["hw_2"]
) as dag:
    
    
    for i in range(10):
        templated_command = dedent('''
            f"echo {i}"
            ''')
        
        task= BashOperator(
        task_id = 'kretov_iteration_' + str(i),
        bash_command = templated_command,
        env = {'i': str(i)}
        )
    
    
    
    def print_n(task_number):
        print(f"task number is {task_number}")
        
    for j in range(10, 30):
            
        task = PythonOperator(
        task_id = "kretov_iteration_" + str(j),
        python_callable = print_n,
        op_kwargs = {'task_number': j}
        )
    
    