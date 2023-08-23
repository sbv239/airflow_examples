from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

def print_variable():
    is_startml = Variable.get('is_startml')
    print(is_startml)

with DAG(
    'hw_efelagereva_12',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}, description = 'a simple dag with variable',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 8, 22)
) as dag:
    t1 = PythonOperator(
        task_id = 'print_variable',
        python_callable = print_variable
    )
