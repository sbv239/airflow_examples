from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_var():
    print(Variable.get('is_startml'))


with DAG(
    'hw_12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw_12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 31),
    catchup=False,
    tags=['hw_12'],
) as dag:
    t = PythonOperator(
        task_id='simple_print',
        python_callable=print_var
    )
    t
