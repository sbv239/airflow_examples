"""
simple dag
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
with DAG(
    'Tima_first_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  #
    },

    description='1_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 6),
    catchup=False,
    tags=['tima'],
) as dag:

    t1 = BashOperator(
        task_id = 'print_directory',
        bash_command = 'pwd'
    )

    def print_date(ds, **kwargs):
        print(kwargs)
        print(ds)
    return 'GOOD JOB'

    t2= PythonOperator(
        task_id='print_the_ds',
        python_callable=print_date,
    )


    t1 >> t2
