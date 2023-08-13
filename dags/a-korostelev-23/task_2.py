from datetime import datetime, timedelta
import fractions
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from aitflow.operators.python import PythonOperator

login = 'a-korostelev-23'

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(f'hw_{login}_2', default_args=default_args) as dag:
    t1 = BashOperator(task_id='print_pwd',
                      bash_command='pwd')
    
    def print_ds(ds):
        print(ds)

    t2 = PythonOperator(task_id='print_ds',
                        python_callable=print_ds)
    
    t1 >> t2
    

