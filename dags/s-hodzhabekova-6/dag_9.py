from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def return_data(ti):
    return "Airflow tracks everything"

    
def pull_operator(ti):
    ate = ti.xcom_pull(
        key = 'return_value',
        task_ids = 'return_data'
    )
    
    print(f'Return Value: {ate}')

with DAG(
    'hw_9_s-hodzhabekova-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_9', 'khodjabekova'],
) as dag:

    t1 = PythonOperator(
        task_id='return_data',
        python_callable=return_data,
    )

    t2 = PythonOperator(
        task_id='pull_data',
        python_callable=pull_operator,

    )

    t1 >> t2