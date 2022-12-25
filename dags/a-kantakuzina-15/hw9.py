"""\
# DAG documentation      
"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def x_push(ti, **kwargs):
    k = kwargs['k']
    v = kwargs['v']
    ti.xcom_push(
        key=k,
        value=v
    )

def x_pull(ti, **kwargs):
    k = kwargs['k']
    res = ti.xcom_pull(
        key=k,
        task_ids='task_push'
    )
    print(f"Pulled value is {res}")

with DAG(
    'kant_hw9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Exercise_9 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 17),
    catchup=False,
    tags=['kant_hw9'],
) as dag:


    def push(ti):
        ti.xcom_push(key='sample_xcom_key', value='xcom test')

    t1 = PythonOperator(
        task_id='push_data',
        python_callable=push,
        )

    def pull(ti):
        t = ti.xcom_pull(key='sample_xcom_key', task_ids='push_data')
        print(t)

    t2 = PythonOperator(
        task_id='pull_data', 
        python_callable=pull,
        )

    t1 >> t2