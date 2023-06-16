from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_context(ds):
    print(ds)

with DAG(
    'hw_2_n-chuviurova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='The first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 15),
    catchup=False,
    tags=['task_02'],
) as dag:

    t1 = BashOperator(
        task_id='hw_n-chuviurova_1',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='hw_n-chuviurova_2',
        python_callable=print_context,
    )

    t1 >> t2