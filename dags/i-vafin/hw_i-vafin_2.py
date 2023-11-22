"""
Task-2: работа с операторами bash и python.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime


def print_ds(ds, **kwargs):
    """
    Выводит логическую дату запуска DAG'а
    """

    print(ds)
    print(kwargs)

    return 'OK'


with DAG(
    'hw_i-vafin_2_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description=__doc__,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 22),
    tags=['hw_i-vafin'],
) as dag:
    dag.doc_md = __doc__

    t1 = BashOperator(
        task_id='print_cur_dir',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds
    )

    t1 >> t2
