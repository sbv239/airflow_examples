from airflow import DAG 
from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_task_number(op_kwargs):
    print (f'task number is: {op_kwargs}')



with DAG(
    'hw_m-kuzina_3', 
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    
    start_date=datetime(2023, 9, 19),
    catchup=False 
) as dag:
    
    
    for i in range(11):
        task = BashOperator(
            task_id='use_i',
            bash_command=f"echo {i}"
        )
        
    for i in range(21):
        task_python = PythonOperator(
        task_id = 'print_num_task',
        python_callable=print_task_number,
        op_kwargs={'i': i}
        )
        
        
    task >> task_python
        
     