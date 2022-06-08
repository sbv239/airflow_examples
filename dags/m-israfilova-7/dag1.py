from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'myseconddag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    }, 
    description='Just for practice',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=[ 'masha' ],
) as dag:

    # t1 = BashOperator(
    #     task_id='print_pwd', 
    #     bash_command='pwd'  
    # )

    # def print_ds(ds, **kwargs):
    #     print(ds)
    #     return ds
        
    # t2 = PythonOperator(
    #     task_id='print_ds',  
    #     python_callable=print_ds
    # ) 

    def numberofthetask(number):
        print (f'task number is: {number}')

    for i in range(5):
        task = PythonOperator(
            task_id='print nuber of the task' + str(i),  
            python_callable=numberofthetask,
            op_kwargs={'number': int(i)},
        )

# t1 >> t2 

