from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator, PythonOperator

def print_ds(ds):
    print(ds)

with DAG('task_1') as dag:
    t1 = BashOperator(
        task_id='print_pwd',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
        retries=3,  # тоже переопределили retries (было 1)
    )
    
    t1>>t2