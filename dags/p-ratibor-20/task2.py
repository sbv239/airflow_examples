from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python.PythonOperator

with DAG(
    'hw_p-ratibor-20_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:
    t1 = BashOperator(
        task_id = 'bash_operator'
        bash_command = 'pwd'
    )
    def print_ds(ds):
        print(ds)

    
    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds
    )

    t1 >> t2