from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from datetime import timedelta, datetime


with DAG(
    'hw_e.mironenko-13_12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
            },
    start_date=datetime(2023, 6, 26),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags = ['e.mironenko-13']
) as dag:
    def print_variable():
        is_startml = Variable.get("is_startml")
        print(is_startml)

    t1 = PythonOperator(
        task_id='task_python_variable',
        python_callable=print_variable,
    )

t1