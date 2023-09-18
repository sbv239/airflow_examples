"""
DAG for task 2
"""
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_t-togyzbaev_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='First task',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['t-togyzbaev']
) as dag:
    t1 = BashOperator(
        task_id='bash_pwd',
        bash_command='pwd'
    )

    def pwd_python(ds):
        print("ds", ds)

    t2 = PythonOperator(
        task_id='python_pwd',
        python_callable=pwd_python
    )

    t1 >> t2
