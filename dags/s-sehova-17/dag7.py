from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from textwrap import dedent


with DAG(
    's-sehova-17-1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),

    catchup=False,
    tags=['example'],
) as dag:
    def print_context(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)
        
    for i in range(20):    
        t72 = PythonOperator(
            task_id=f"t3_pt_{i}",
            python_callable=print_context,
            op_kwargs = "task number is: i"
        )

t72
