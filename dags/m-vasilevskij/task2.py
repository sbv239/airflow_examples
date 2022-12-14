from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


def print_ds(ds):
    print(ds)
    return 'ds printed'

with DAG(

    'task2',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
) as dag:

    t1 = BashOperator(
        task_id = 'print_pwd',
        bash_command = 'pwd'
    )

    t2 = PythonOperator(
        task_id = 'print_ds',
        python_callable = print_ds

    )

    t1 >> t2





