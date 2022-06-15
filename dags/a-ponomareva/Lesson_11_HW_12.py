from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from datetime import datetime, timedelta

"""
Напишите DAG, состоящий из одного PythonOperator. Этот оператор должен печатать значение Variable с названием is_startml.
"""
def print_var():
    is_startml = Variable.get('is_startml')
    print(is_startml)

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'DAG_HW_10_ponomareva',
    default_args=default_args,
    description='DAG for HW_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=['ponomareva'],
) as dag:

    task = PythonOperator(
        task_id = 'print_var',
        python_callable=print_var,
    )