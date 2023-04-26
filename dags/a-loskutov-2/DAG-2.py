"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    ' hw_a-loskutov_2',
    # Параметры по умолчанию для тасок

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='My first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 25),
    catchup=False,
    tags=['Loskutov_hm_2'],
) as dag:
    

    t1_Bash = BashOperator(
        task_id='print_pwd',  # id, будет отображаться в интерфейсе
        bash_command="pwd",  # какую bash команду выполнить в этом таске
    )

    def print_context(ds, **kwargs):
            print(ds)
            print('My first DAG')
            return 'Whatever you return gets printed in the logs'

    t2_python = PythonOperator(
        task_id='print_the_context',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )


    t1_Bash >> t2_python

