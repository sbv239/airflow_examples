"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'first_bash_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='simple bash dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 7, 27),
        catchup=False,
        tags=['ag'],
) as dag:
    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
    )


    def print_ds(ds, **kwargs):
        print(ds)
        return 'py operator has done the work!'


    t22 = PythonOperator(
        task_id='print_ds_and_something_else',
        python_callable=print_ds
    )

    # А вот так в Airflow указывается последовательность задач
    t1 >> t22
    # будет выглядеть вот так
    #      -> t2
    #  t1 |
    #      -> t3
