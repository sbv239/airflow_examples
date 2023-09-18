"""
DAG for task 12
"""
from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

with DAG(
        'hw_t-togyzbaev_12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['t-togyzbaev']
) as dag:
    def print_var():
        print(Variable.get("is_startml"))


    t1 = PythonOperator(
        task_id="print_var",
        python_callable=print_var
    )
