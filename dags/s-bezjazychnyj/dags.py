from airflow import DAG
from textwrap import dedent
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_s-bezjazychnyj',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 5),
    catchup=True,
    tags=['bezsmen'],
) as dag:
    t1=BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
    )
    def print_ds(ds,**kwargs):
        print(ds)
    t2=PythonOperator(
        task_id='prind_ds',
        python_callable=print_ds,
    )
    t1>>t2
