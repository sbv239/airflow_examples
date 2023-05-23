from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

"""
Напишите DAG, состоящий из одного PythonOperator. Этот оператор должен печатать значение Variable с названием is_startml

NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
"""


def get_variables():
    from airflow.models import Variable

    is_startml = Variable.get('is_startml')
    return is_startml


with DAG(
    'hw_d-ivashkin-23_12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework 12-th step DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 21),
    catchup=False,
    tags=['homework', 'di']
) as dag:

    task = PythonOperator(
        task_id='get_variables',
        python_callable=get_variables
    )

task
