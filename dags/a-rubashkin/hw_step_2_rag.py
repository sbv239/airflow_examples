from datetime import datetime, timedelta
# from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_logic_date(ds):
	print(ds)

with DAG(
    'rag_hw_2',
    default_args={
        'depends_in_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='HW_step_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 04, 01),
    catchup=False,
    tags=['rag23']
) as dag:
    
    t1 = BashOperator(
        task_id='show_pwd',
        bash_command='pwd'
    )
    
    t2 = PythonOperator(
        task_id='current_logical_date',
        python_callable=print_logic_date
    )

t1 >> t2