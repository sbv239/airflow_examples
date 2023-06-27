from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
with DAG(
    'hw_a-shulga-4_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 26),
    catchup=False
    ) as dag:
        
        for i in range(10):
                t1 = BashOperator(
                   task_id = f'bash_{i}',
                   bash_command = f"echo {i}"
                )

        def print_task(task_number):
                print(f'task number is: {task_number}')

        for i in range(20):
                t2 = PythonOperator(
                        task_id = f'python_{i}',
                        python_callable=print_task,
                        op_kwargs={'task_number': int(i)}                
                        )

        t1 >> t2