from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def print_var():
    print(Variable.get('is_startml'))


with DAG(
        '11_dm-morozov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 2, 14)
) as dag:
    task = PythonOperator(
        task_id='simple_connection',
        python_callable=print_var
    )

    task
