from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def check_num(task_number):
    return f'task number is: {task_number}'

    
with DAG(
    'hw_2-maks-novikov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    
    description='Try a new dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 16),
    catchup=False,
    tags=['hw_2-maks-novikov'],
) as dag:
    
    for i in range(1, 11):
        t1 = BashOperator(
            task_id='task_' + str(i),
            bash_command=f'echo {i}',
            dag=dag
        )
        
    for i in range(11, 31):
        t2 = PythonOperator(
            task_id='task_' + str(i),
            python_callable=check_num,
            op_kwargs={'task_number': i}
        )


t1 >> t2


    
