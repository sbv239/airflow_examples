from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'hw_7_m-korablin',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='my_first_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['VanDarkholme'],
) as dag:
    
    def get_task_number(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(f"ts: {ts}")
        print(f"run_id: {run_id}")
        
    for i in range(30):
        tP = PythonOperator(
            task_id=f'python_task_{i}',
            python_callable=get_task_number,
            op_kwargs={'task_number': i}
        )
    tP
    