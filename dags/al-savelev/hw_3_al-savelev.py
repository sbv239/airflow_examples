from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_3_al-savelev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='test_11_3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['hw_3_al-savelev']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'hw_3_al-savelev_bash_{i}',
            bash_command=f'echo {i}')

    def print_task(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        t2 = PythonOperator(
            task_id=f'hw_3_al-savelev_python_{i}',
            python_callable=print_task,
            op_kwargs={'task_number': i})

    t1 >> t2