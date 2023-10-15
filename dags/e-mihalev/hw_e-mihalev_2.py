from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_e-mihalev_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple DAG',
    start_date=datetime(2023, 10, 14),
    catchup=False,
    tags=['example']
) as dag:
    date = "{{ ds }}"
    t1 = BashOperator(task_id='print_current_directory', bash_command='pwd')
    def print_ds(ds):
        return ds
    t2 = PythonOperator(task_id='print_date', python_callable=print_ds)
    t1 >> t2
