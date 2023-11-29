from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

def number(task_number):
    return f'task number is: {task_number}'

with DAG(
    'hw_ni-nikitina_4', 
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Second Task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 29),
    catchup=False
) as dag:
    
    for i in range(10):
	    t1 = BashOperator(
            task_id=f'hw_4_nn_bo_{i}',
            bash_command=f"echo {i}"
        )
    
    t1.doc_md = dedent(
        '''
        ####Task Documentation
        #indent `code elements` **thick** *cursive*
        '''
    )
    
    for j in range(20):
	    t2 = PythonOperator(
            task_id=f'hw_4_nn_po_{j}',
            python_callable=number,
            op_kwargs={'task_number': j}
        )
    
    t2.doc_md = dedent(
        '''
        ####Task Documentation
        #indent `code elements` **thick** *cursive*
        '''
    )