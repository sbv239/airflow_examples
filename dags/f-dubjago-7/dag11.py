"""
Variables
"""
from airflow import DAG
from airflow.models import Variable

from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator



default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def print_var():
    var = Variable.get("is_startml")
    print(var)


with DAG(
        'f-dubjago-7_dag11',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 10),
        tags=['df'],
) as dag:
    print_var = PythonOperator(
        task_id='print_var',
        python_callable=print_var,
    )
    print_var
















