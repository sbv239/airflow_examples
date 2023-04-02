from airflow import DAG
from airflow.operators.bash import BashOperator, PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

def print_ds(ds):
    """Печатаем переменную ds"""
    print(ds)
    return 'Вот, что я вернул'

with DAG(
    'task_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='dag for task 2',
    schedule_interval=timedelta,
    start_date=datetime(2023,4,2),
    catchup=False,
    tags=['task_2']
) as dag:
    bash_task = BashOperator(
        task_id = 'bash_task',
        bash_command = 'pwd'
    )
    pyth_task = PythonOperator(
        task_id = 'pyth_task',
        python_callable=print_ds
    )
    bash_task >> pyth_task