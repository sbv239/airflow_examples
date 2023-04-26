"""
step_12 DAG
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable

from airflow.operators.python import PythonOperator


with DAG(
    'hw_r-shahvaly_12',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG for step_12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['r-shahvaly'],
) as dag:

    def print_is_startml():
        is_startml = Variable.get("is_startml")
        print(is_startml)

    task = PythonOperator(
        task_id="PythonOperator",
        python_callable=print_is_startml,
    )