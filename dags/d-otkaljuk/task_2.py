from datetime import datetime, timedalta
from textwrap import dedent

from airflow import DAG #импортируем класс DAG для объявления
from airflow.operators.bash import BashOperator #импортируем BashOperator
from airflow.operators.python import PythonOperator #импортруем PythonOperator


with DAG(
    "hw_2_d_otkaljuk",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        description='task 1. BashOperator and PythonOperator',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['t_2_d_otkal'],
) as dag:
        
        t_1 = BashOperator(
                task_id = 'Bash_1'
                bash_command = 'pwd'
        )

        def print_context(ds, **kwargs):
            """Пример PythonOperator"""
            print(ds)
        
        t2 = PythonOperator(
            task_id = 'PythOperator print_ds',  # нужен task_id, как и всем операторам
            python_callable = print_context,  # свойственен только для PythonOperator - передаем саму функцию
        )
        
        t1 >> t2