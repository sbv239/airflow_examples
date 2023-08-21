from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def print_ds(ds, **kwargs):
    print(ds)

with DAG(
    'hw_efelagereva_2',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}, description = 'a simple DAG',
schedule_interval = timedelta(days = 1),
start_date = datetime(2023, 8, 20))as dag:
    t1 = BashOperator(
        task_id = 'print_directory',
        bash_command = 'pwd'
    )
    t2 = PythonOperator(
        task_id = 'print_ds',
        python_callable = print_ds,
        op_kwargs = {'ds' : '{{ds}}'}
    )
    t1 >> t2
