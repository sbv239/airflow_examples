from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_arse-beljaev_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        },
    description='hw_2_lesson_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 17),
    catchup=False,
    tags=['example']
    ) as dag:
    def print_context(ds, **kwargs):
        """PythonOperator"""
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_context,
    )
    
    t1 = BashOperator(
        task_id='execute_pwd',
        bash_command='pwd',
    )

    t1 >> t2

