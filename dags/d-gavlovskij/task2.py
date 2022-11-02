from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'task2_d-gavlovskij',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
    tags=['gavlique']
) as dag:
    def print_ds(ds):
        print(ds)
    
    t1 = BashOperator(
        task_id='pwd',
        bash_command='pwd',
        dag=dag
    )

    t2 = PythonOperator(
        taskid='print_ds',
        python_callable=print_ds,
        dag=dag
    )

    t1 >> t2
