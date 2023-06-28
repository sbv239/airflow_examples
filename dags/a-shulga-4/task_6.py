from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
with DAG(
    'hw_a-shulga-4_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 28),
    catchup=False
    ) as dag:
        
        for i in range(10):
                t1 = BashOperator(
                   task_id = f'bash_{i}',
                   bash_command = f'echo $NUMBER',
                   env={'NUMBER': i}
                )

        t1