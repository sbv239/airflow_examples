from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_context(ds, **kwargs):
    print(ds)
    return 'python operator has done'


with DAG(
    'd_kurm_hw1',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)},
    description='An exersize 1 DAG',
    ) as dag:
    
    t1 = BashOperator(
        task_id='bash_pwd',
        bash_command='pwd',
        )
    t2 = PythonOperator(
        task_id='python_print_context',
        python_callable=print_context,
        )

    t1 >> t2

